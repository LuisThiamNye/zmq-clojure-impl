(ns crypticbutter.zmq.impl.ctx
  (:require
   [farolero.core :as f :refer [signal error warn]]
   [crypticbutter.zmq.socket :as sock]))

(derive ::error.context-terminated :error/conflict)
(derive ::error.socket-limit-reached :error/conflict)

(def term-thread-id 0)
(def reaper-thread-id 1)

(defrecord CtxState
           [sockets
            ;; empty-slots
            terminating?
            selectors
            reaper
            io-threads
            slot-count
            slots
            endpoints
            max-socket-id
            socket-limit
            io-thread-limit
            block-on-term?
            pending-connections
            ipv6?])

(deftype Ctx
         [^:volatile-mutable ^clojure.lang.Keyword stage ;; :active -> :terminating -> :dead
          term-mailbox
          ^clojure.lang.Atom *state]
  (active? [_] (= :active stage))
  (-add-io-thread! [_]
    (let [thread (io-thread/new-io-thread)]
      (swap! *state :io-threads conj thread)
      thread))
  (choose-io-thread [this affinity]
    (let [{:keys [io-threads io-thread-limit]} @*state]
      (reduce (fn [winner n]
                (if (not (or (nil? affinity) (affinity n)))
                  winner
                  (if-some [thread (nth io-threads n nil)]
                    (if (nil? thread)
                      (reduced (-add-io-thread! this))
                      (if (> (:iothread/load winner)
                             (:iothread/load thread))
                        winner
                        thread)))))
              nil
              (range (count io-thread-limit)))))
  (create-socket [_ sock-type]
    (case stage
      :active
      (if (< (count sockets) socket-limit)
        (let [thread (poll-last empty-slots)
              id (inc max-socket-id)
              socket (sock/socket sock-type thread id)]
          ;; TODO restore thread if sock is nil & return
          (swap! *state
                 (fn [s]
                   (-> s
                       (update :sockets conj socket)
                       (update :slots assoc thread (sock/get-mailbox socket)))))
          socket)
        (error ::error.socket-limit-reached
               {:message "The limit on the total number of open ZMQ sockets has been reached"}))

      (:terminating :dead)
      (error ::error.context-terminated
             {:message "The ZMQ context associated with the specified socket was terminated."})))
  (terminate! [_]
    (set! stage :terminating)
    (try (doseq [conn (:pending-connections @*state)]
           (let [s (sock/socket :pair)]
             (sock/bind s conn)
             (sock/close s)))
         (doseq [s (:sockets @*state)]
             ;; TODO send stop
           )
         (finally))

      ;; TODO wait until reaper thread closed all sockets
      ;; (<!! )
    (mailbox/recv!! term-mailbox) ;; recv done cmd

    (try
      (set! alive? false)
      ;; stop all io thread
      ;; close all io thread
      ;; close all selectors
      ;; close reaper
      ;; close term mailbox
      ;; TODO
      (let [[old new] (swap-vals! *state
                                  (fn [state]
                                    (-> state
                                        (update :selectors empty)
                                        (update :io-threads empty))))])
      (catch java.io.IOException e
        ;; TODO throw as custom io exception
        ))

    (set! stage :dead)))
