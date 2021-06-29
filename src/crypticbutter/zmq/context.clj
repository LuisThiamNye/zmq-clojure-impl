(ns crypticbutter.zmq.context
  (:require
   [clojure.core.async :as async :refer [go go-loop <! <!! >! >!!]]
   [farolero.core :as f :refer [signal error warn]]
   [taoensso.encore :as enc]
   [crypticbutter.zmq.socket :as sock])
  (:import
   (java.nio.channels SelectableChannel
                      SelectionKey
                      Selector)))

(defonce contexts (atom {}))

(defrecord Handle
           [sc poll-events-handler ops cancelled]
  ())

(defn new-handle [{:keys [sc poll-events-handler] :as fields}]
  {:pre [(some? sc) (some? poll-events-handler)]}
  (map->Handle fields))

(defn process-retired []
  (when @retired?
    (vreset! retired? false)
    (doseq [handle fdTable]
      (let [selection-key (handle.fd.keyfor selector)]
        (if (or (cancelled? handle)
                (not (fd.isopen? handle)))
          (do
            (when (some? selection-key)
              (.cancel selection-key))
                  ;; TODO remove handle from fd table
            )
          (if (nil? selection-key)
            (when (fd.isopen? handle)
              (try
                      ;; key = handle.fd.register (selector, handle.ops, handle);
                      ;; assert (key != null);
                (catch java.nio.channels.CancelledKeyException e
                  (.printStackTrace e))
                (catch java.nio.channels.ClosedChannelException e
                  (.printStackTrace e))
                (catch java.nio.channels.ClosedSelectorException e
                  (.printStackTrace e))))
            (when (.isValid selection-key)
              (.interestOps selection-key (:ops handle)))))))))



;; (defn destroy-poller []
;;   (.close selector))

(let [thread-name (str (::io-thread-name-prefix ctx) thread-id)
      worker (Thread. (poller) thread-name)]
  (.setDaemon worker true)
  (.start worker))

(.addShutdownHook
 (Runtime/getRuntime)
 (Thread.
  (fn [] ;; TODO shut down all contexts
    )))

(defrecord IOThread
           [status thread-obj mailbox])

(defn create-context
  ([options]
   (let [opts (enc/merge {::io-thread-count 1
                          ::io-thread-name-prefix "zmq-io-thread-"
                          ::max-sockets 1024
                          ::ipv6-enabled false
                          ::block-on-termination true}
                         options)]
     (atom {:ctx/io-thread-name-prefix (::io-thread-name-prefix opts)
            :ctx/max-sockets (::max-sockets 1024 opts)
            :ctx/ipv6-enabled (::ipv6-enabled opts)
            :ctx/block-on-termination (::block-on-termination opts)
            :ctx/io-threads (into {}
                                  (map (fn [n]
                                         [(keyword (name (::io-thread-name-prefix opts))
                                                   n)
                                          (map->IOThread {:status :init})]))
                                  (range (::io-thread-count opts)))}))))



(comment
  {::io-thread-count 1
   ::io-thread-name-prefix "io-thread-"
   ::max-sockets 1024
   ::ipv6-enabled false
   ::block-on-termination true}

  ;;
  )
