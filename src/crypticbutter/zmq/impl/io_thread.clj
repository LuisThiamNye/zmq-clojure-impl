(ns crypticbutter.zmq.impl.io-thread
  (:require
   [crypticbutter.zmq.impl.cmd :as cmd]
   [crypticbutter.zmq.impl.mailbox :as mbox]
   [crypticbutter.zmq.impl.poller :as poller])
  (:import
   (crypticbutter.zmq.impl.mailbox Mailbox)
   (crypticbutter.zmq.impl.poller Poller)))

(defprotocol IOThreadProtocol
  (get-load [_])
  (start! [_])
  (stop! [_])
  (close! [_]))

(deftype IOThread
  [^:string title
   ^Mailbox mailbox
   ^Poller poller
   ^java.lang.Thread poller-worker]

  cmd/Commandable
  (handle-cmd [_ cmd _]
    (case cmd
      ::cmd/stop
      (do
        (poller/remove-handle! poller (mbox/handle mailbox))
        (poller/stop! poller))))

  poller/HandlesEventIn
  (handle-in [_]
    (loop []
      (when-some [cmd (mbox/recv! mailbox 0)]
        (cmd/execute! cmd))))

  IOThreadProtocol
  (get-load [_] (poller/get-load poller))
  (start! [_]
    (.start poller-worker))
  (close! [_]
    (poller/terminate! poller)
    (mbox/close! mailbox))
  (stop! [this]
    (mbox/send! mailbox (cmd/new-cmd ::cmd/stop this))))

(defn new-io-thread [id]
  (let [title (str #_(::io-thread-name-prefix opts) ;; TODO
               "io-thread-" id)
        poller (poller/new-poller)
        mailbox (mbox/new-mailbox title)
        handle (mbox/handle mailbox)
        thread (->IOThread title
                           mailbox
                           poller
                           (doto (Thread. poller title)
                             (.setDaemon true)))]
    (doto poller
      (poller/add-handle! handle thread)
      (poller/set-poll-in! handle))))
