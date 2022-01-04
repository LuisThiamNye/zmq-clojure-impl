(ns crypticbutter.zmq.impl.ctx
  (:require
   [farolero.core :as f :refer [signal error warn]]
   [crypticbutter.zmq.impl.cmd :as cmd]
   [crypticbutter.zmq.impl.mailbox :as mbox]
   [crypticbutter.zmq.impl.io-thread :as io-thread]
   [crypticbutter.zmq.impl.reaper :as reaper]
   [crypticbutter.zmq.impl.socket.base :as base])
  (:import
   (java.util.concurrent.locks Lock)
   (crypticbutter.zmq.impl.mailbox Mailbox)))

(derive ::error.context-terminated :error/conflict)
(derive ::error.socket-limit-reached :error/conflict)

(def term-thread-id 0)
(def reaper-thread-id 1)

(defrecord CtxState
  [sockets
            ;; empty-slots
   terminating?
   io-threads
   slot-count
   slots
   endpoints
   last-socket-id
   socket-limit
   io-thread-limit
   block-on-term?
   pending-connections
   ipv6?])

(defprotocol CtxProtocol
  (active? [_])
  (-add-io-thread! [_ n])
  (choose-io-thread [_ affinity])
  (create-socket! [_ creator])
  (-destroy-socket! [_ socket])
  (close-socket! [_ socket])
  (terminate! [_]))

(deftype Ctx
  [^:volatile-mutable ^clojure.lang.Keyword stage ;; :active -> :terminating -> :dead
   ^clojure.lang.Atom *uninitialized?
   ^Mailbox term-mailbox
   ^clojure.lang.Atom *state
   ^:volatile-mutable reaper]

  CtxProtocol
  (active? [_] (= :active stage))
  (-add-io-thread! [_ n]
    (let [thread (io-thread/new-io-thread n)]
      (swap! *state update :io-threads assoc n thread)
      thread))
  (choose-io-thread [this affinity]
    (let [{:keys [io-threads io-thread-limit]} @*state
          thread-count (count io-thread-limit)]
      (loop [winner nil
             n 0]
        (if (< n thread-count)
          (if (not (or (nil? affinity) (affinity n)))
            (recur winner (inc n))
            (if-some [thread (nth io-threads n nil)]
              (recur (if (< (:iothread/load thread) (:iothread/load winner))
                       winner thread)
                     (inc n))
              (-add-io-thread! this n)))
          winner))))
  (create-socket! [_ creator]
    (case stage
      :active
      (let [[_ new]
            (swap-vals!
             *state
             (fn [{:keys [sockets socket-limit last-socket-id]
                   :as state}]
               (if (< (count sockets) socket-limit)
                 (let [id (inc last-socket-id)
                       socket (base/create-socket creator id)]
                   (-> state
                       (update :sockets conj socket)))
                 (error ::error.socket-limit-reached
                        {:message "The limit on the total number of open ZMQ sockets has been reached"}))))
            sockets (:sockets new)]
        (when (compare-and-set! *uninitialized? true false)
          (set! reaper (reaper/new-reaper term-mailbox))
          (reaper/start! reaper))
        (nth sockets (dec (count sockets))))

      (:terminating :dead)
      (error ::error.context-terminated
             {:message "The ZMQ context associated with the specified socket was terminated."})))
  (-destroy-socket! [_ socket]
    (let [{:keys [sockets]} (swap! *state update :sockets
                                   (fn [socks]
                                     (into [] (remove #{socket}) socks)))]
      (when (empty? sockets)
        (reaper/stop! reaper))))
  #_(connect! [this ctx addr] ;; TODO
    (when-not (contains? endpoints addr)
      (if-some [io-thread (ctx/choose-io-thread ctx (:affinity options))]
        (let [session (new-req-session)
              pipe (when (:only-fill-connected? options)
                     (let [pipe-pair (pipe/new-pipe-pair (mbox/get-mailbox socket-base)
                                                         (mbox/get-mailbox io-thread)
                                                         (:send-msg-limit options)
                                                         (:recv-msg-limit options)
                                                         false false)

                           pipe (aget pipe-pair 0)]
                       (sess/attach-pipe! session (aget pipe-pair 1))
                       (base/attach-pipe! this pipe {:locally-initiated? true})
                       pipe))]
          (base/add-endpoint! socket-base addr session pipe))
        (error ::error.no-io-thread
               {:message "No I/O thread is available to accomplish the task."}))))
  (close-socket! [_ socket]
    (base/close! (base/get-base socket)
                 #(mbox/send! (mbox/get-mailbox reaper) (cmd/new-cmd ::cmd/reap reaper))))
  (terminate! [this]
    (let [first-attempt? (= :terminating stage)
          _       (set! stage :terminating)
          [old _] (swap-vals! *state
                              (fn [state]
                                (-> state
                                    (update :io-threads empty))))
          sockets (:sockets old)
          io-threads (:io-threads old)]
      (when first-attempt?
        (doseq [s sockets]
          (-destroy-socket! this s)
          (mbox/send! (mbox/get-mailbox s) (cmd/new-cmd ::cmd/stop s)))
        (when (empty? sockets)
          (reaper/stop! reaper)))

      ;; wait until reaper thread closed all sockets
      ;; recv done cmd
      (when (nil? (mbox/recv! term-mailbox -1))
        (error "wot"))

      (try
        (doseq [thread io-threads]
          (io-thread/stop! thread))
        (doseq [thread io-threads]
          (io-thread/close! thread))
        (when (some? reaper)
          (reaper/close! reaper))
        (mbox/close! term-mailbox)
        (catch java.io.IOException e
          (throw e)))

      (set! stage :dead))))

(defn new-ctx []
  (->Ctx :active
         (atom true)
         (mbox/new-mailbox "terminator")
         (atom {})
         nil))
