(ns crypticbutter.zmq.impl.reaper
  (:require
   [crypticbutter.zmq.impl.cmd :as cmd]
   [crypticbutter.zmq.impl.mailbox :as mbox :refer [HasMailbox]]
   [crypticbutter.zmq.impl.poller :as poller]
   [crypticbutter.zmq.impl.socket.base :as base])
  (:import
   (crypticbutter.zmq.impl.mailbox Mailbox)
   (crypticbutter.zmq.impl.poller Poller)
   (crypticbutter.zmq.impl.socket.base SocketBase)))

(defprotocol ReaperProtocol
  (start! [_])
  (stop! [_])
  (close! [_])
  (-terminate! [_]))

(deftype Reaper
  [^:string title
   ^Mailbox mailbox
   ^Poller poller
   ^java.lang.Thread poller-worker
   ^clojure.lang.Atom *terminating?
   ^Mailbox terminator-mailbox
   ^:unsynchronized-mutable ^:int socket-count]

  HasMailbox
  (get-mailbox [_] mailbox)

  ReaperProtocol
  (start! [_] (.start poller-worker))
  (stop! [this]
    (when-not @*terminating?
      (mbox/send! mailbox (cmd/new-cmd ::cmd/stop this))))
  (close! [_]
    (poller/terminate! poller)
    (mbox/close! mailbox))
  (-terminate! [_]
    (mbox/send! terminator-mailbox (cmd/new-cmd ::cmd/done nil))
    (poller/remove-handle! poller (mbox/handle mailbox))
    (poller/stop! poller))

  poller/HandlesEventIn
  (handle-in [_]
    (loop []
      (when-some [cmd (mbox/recv! mailbox 0)]
        (cmd/execute! cmd))))

  cmd/Commandable
  (handle-cmd [this cmd data]
    (case cmd
      ::cmd/reap
      (let [socket-base ^SocketBase data
            socket-mbox (mbox/get-mailbox socket-base)
            thread-safe? (base/thread-safe? socket-base)
            handle (if thread-safe?
                     (base/init-reaper-signaller! socket-base)
                     (mbox/handle socket-mbox))]
        (set! socket-count (inc socket-count))
        (base/set-destroy-hook!
         socket-base (fn []
                       (poller/remove-handle! poller (mbox/handle mailbox))
                       (mbox/send! mailbox (cmd/new-cmd ::cmd/reaped this))))
        (doto poller
          (poller/add-handle! handle socket-base)
          (poller/set-poll-in! handle))
        (mbox/send! mailbox (cmd/new-cmd ::cmd/term-req this socket-base))
        (base/maybe-destroy! socket-base))

      ::cmd/reaped
      (do (set! socket-count (dec socket-count))
          (when (and (zero? socket-count) @*terminating?)
            (-terminate! this)))

      ::cmd/stop
      (do (reset! *terminating? true)
          (when (zero? socket-count)
            (-terminate! this))))))

(defn new-reaper [id term-mbox]
  (let [title (str #_(::io-thread-name-prefix opts) ;; TODO
               "reaper-" id)
        mailbox (mbox/new-mailbox title)
        handle (mbox/handle mailbox)
        poller (poller/new-poller)
        reaper (->Reaper title
                         mailbox
                         poller
                         (doto (Thread. poller title)
                           (.setDaemon true))
                         (atom false)
                         term-mbox
                         0)]
    (doto poller
      (poller/add-handle! handle reaper)
      (poller/set-poll-in! handle))))
