(ns crypticbutter.zmq.impl.poller
  (:require
   [farolero.core :refer [error]])
  (:import
   (java.lang Thread)
   (java.nio.channels Selector SelectableChannel SelectionKey)
   (java.util.concurrent CountDownLatch)))

(defprotocol HandlesEventIn
  (handle-in [_]))
(defprotocol HandlesEventTimer
  (handle-timer [_]))
(defprotocol X
  (handle-out [_])
  (handle-accept [_])
  (handle-connect [_]))

(defprotocol PollerProtocol
  (-rebuild-selector! [_])
  (-maybe-rebuild-selector! [_ r timeout start-timestamp])
  (get-load [_])
  (start! [_])
  (stop! [_])
  (add-handle! [_ handle event-sink])
  (remove-handle! [_ handle])
  (set-op! [_ handle ops reset?])
  (set-poll-in! [_ handle])
  (reset-poll-in! [_ handle])
  (set-poll-out! [_ handle])
  (reset-poll-out! [_ handle])
  (set-poll-connect! [_ handle])
  (set-poll-accept! [_ handle])
  (terminate! [_]))

(defrecord HandleData [^:int ops removed? events])

(deftype Poller
  [^clojure.lang.Atom running?
   ^CountDownLatch stop-latch
   ^:volatile-mutable some-retired?
   ^:volatile-mutable ^Selector selector
   ^clojure.lang.Atom handles]

  PollerProtocol
  (get-load [_] (count @handles))
  (add-handle! [_ handle event-sink]
    (swap! handles assoc handle (map->HandleData {:events event-sink})))
  (remove-handle! [_ handle]
    (swap! handles update handle
           (fn [h] (assoc h :removed? true)))
    (set! some-retired? true))
  (set-op! [_ handle ops reset?]
    (swap! handles update handle
           #(update % (if reset? bit-and bit-or) ops))
    (set! some-retired? true))
  (set-poll-in! [this handle] (set-op! this handle SelectionKey/OP_READ false))
  (reset-poll-in! [this handle] (set-op! this handle SelectionKey/OP_READ true))
  (set-poll-out! [this handle] (set-op! this handle SelectionKey/OP_WRITE false))
  (reset-poll-out! [this handle] (set-op! this handle SelectionKey/OP_WRITE true))
  (set-poll-connect! [this handle] (set-op! this handle SelectionKey/OP_CONNECT false))
  (set-poll-accept! [this handle] (set-op! this handle SelectionKey/OP_ACCEPT false))
  (stop! [_]
    (reset! running? false)
    (set! some-retired? false)
    (.wakeup selector))
  (terminate! [this]
    (stop! this)
    (.await stop-latch)
    (.close selector))
  (-rebuild-selector! [_]
    (let [old-selector selector
          new-selector (Selector/open)]
      (set! selector new-selector)
      (set! some-retired? true)
      (.close old-selector)))
  (-maybe-rebuild-selector! [this r timeout start-timestamp]
    (let [r' (if (or (zero? timeout)
                     (< (- (System/currentTimeMillis) start-timestamp) (/ timeout 2)))
               (inc r)
               0)]
      (if (< 10 r')
        (do (-rebuild-selector! this)
            0)
        r')))

  Runnable
  (run [this]
    (let [counter (volatile! 0)]
      (while @running?
        (let [timeout 0 ;; TODO (execute-timers!)
              ]
          (when some-retired?
            (set! some-retired? false)
            (doseq [handlekv @handles]
              (let [^SelectableChannel handle   (key handlekv)
                    {:keys [removed?] :as handle-data} ^HandleData (val handlekv)
                    selection-key             (.keyfor handle selector)]
                (if (or removed?
                        (not (.isOpen handle)))
                  (do
                    (swap! handles dissoc handle)
                    (when (some? selection-key)
                      (.cancel selection-key)))
                  (let [ops (:ops handle-data)]
                    (if (nil? selection-key)
                      (when (.isOpen handle)
                        (try
                          (.register handle selector ops handle)
                          (catch java.nio.channels.CancelledKeyException e
                            (.printStackTrace e))
                          (catch java.nio.channels.ClosedChannelException e
                            (.printStackTrace e))
                          (catch java.nio.channels.ClosedSelectorException e
                            (.printStackTrace e))))
                      (when (.isValid selection-key)
                        (.interestOps selection-key ops))))))))
          (let [start (System/currentTimeMillis)]
            (try
              (let [rc (.select selector timeout)]
                (if (zero? rc)
                  (vreset! counter (-maybe-rebuild-selector! this @counter timeout start))
                  (let [^java.util.Set selected-keys (.selectedKeys selector)]
                    (doseq [selected-key selected-keys]
                      (let [pollset-handle (.attachment selected-key)
                            {:keys [events removed?]} (get @handles pollset-handle)]
                        (.remove selected-keys selected-key)
                        (when-not removed?
                          (try
                            (when (.isValid selected-key)
                              (when (.isAcceptable selected-key)
                                (handle-accept events))
                              (when (.isConnectable selected-key)
                                (handle-connect events))
                              (when (.isWritable pollset-handle)
                                (handle-out events))
                              (when (.isReadable pollset-handle)
                                (handle-in ^HandlesEventIn events)))
                            (catch java.nio.channels.CancelledKeyException e
                              (.printStackTrace e))
                            (catch RuntimeException e
                              (.printStackTrace e)))))))))
              (catch java.nio.channels.ClosedSelectorException e
                (-rebuild-selector! this)
                (.printStackTrace e)
                (error "EINTR") ;; TODO
                ))))))
    (.countDown stop-latch)))

(defn new-poller []
  (->Poller
   true ;; running
   (CountDownLatch. 1) ;; used to block until poller finished
   false ;; some event sources retired?
   (Selector/open)
   {}))
