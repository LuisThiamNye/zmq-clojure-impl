(ns crypticbutter.zmq.impl.socket.base
  (:require
   [clojure.string :as str]
   [farolero.core :as f :refer [signal error warn]]
   [taoensso.encore :as enc]
   [crypticbutter.snoop :refer [>defn => >defn-]]
   [crypticbutter.zmq.impl.cmd :as cmd :refer [Commandable]]
   [crypticbutter.zmq.impl.poller :as poller]
   [crypticbutter.zmq.impl.pipe :as pipe]
   [crypticbutter.zmq.impl.mailbox :as mbox :refer [MailboxProtocol]]
   [crypticbutter.zmq.impl.msg :as msg]
   [crypticbutter.zmq.impl.util :as util])
  (:import
   (java.util HashSet)
   (java.util.concurrent.locks ReentrantLock)
   (java.nio.channels ServerSocketChannel SocketChannel Pipe$SourceChannel)
   (java.net InetAddress
             StandardSocketOptions)
   (crypticbutter.zmq.impl.pipe Pipe)
   (crypticbutter.zmq.impl.mailbox Signaller)))

(derive ::error.invalid-endpoint :error/incorrect)
(derive ::error.unsupported-protocol :error/unsupported)
(derive ::error.incompatible-protocol :error/unsupported)
(derive ::error.address-in-use :error/conflict)
(derive ::error.address-not-local :error/unsupported) ;; ADDRNOTAVAIL The requested address was not local.
(derive ::error.device-not-found :error/unavailable)
(derive ::error.context-terminated :error/conflict)
(derive ::error.invalid-socket :error/incorrect)
(derive ::error.no-io-thread :error/unavailable)

  ;; unavailable (server not there)
  ;; interrupted
  ;; incorrect
  ;; forbidden
  ;; unsupported
  ;; not-found (resource)
  ;; conflict (with state of system)
  ;; fault (something went wrong that we do not know)
  ;; busy

(defprotocol Socket
  (get-base [_])
  (send! [_ msg])
  (recv! [_])
  (get-options [_])
  (awaiting-more? [_])
  (attach-pipe! [_ pipe options]))

(defprotocol SocketCreator
  (create-socket [_ id]))

(def ^:const max-cmd-delay 3000000)

(deftype EndpointWithPipe
  [endpoint
   ^Pipe pipe])

(defprotocol IsSocketBase
  (thread-safe? [_])
  (register-pipe! [_ pipe socket])
  (maybe-terminate-pipe! [_ pipe])
  (add-endpoint! [_ addr endpoint pipe])
  (terminate-endpoint! [_ addr])
  (-process-commands! [_ timeout throttle])
  (remove-pipe! [_ pipe])
  (init-reaper-signaller! [_] "Returns the new signaller's handle")
  (close! [_ after-hook] "After hook may get called within a lock")
  (stop! [_])
  (set-destroy-hook! [_ hook])
  (maybe-destroy! [_]))

(defmacro with-sock-lock [socket-base & body]
  `(do (when (.thread-safe? ~socket-base)
         (.lock (.-lock ~socket-base)))
       (try
         ~@body
         (finally (when (.thread-safe? ~socket-base)
                    (.unlock (.-lock ~socket-base)))))))

(deftype SocketBase
  ;; Encapsulates common socket functionality
  [mailbox
   ^:unsynchronized-mutable ^clojure.lang.PersistentHashMap endpoints
   ^:unsynchronized-mutable ^:boolean alive?
   ^:unsynchronized-mutable ^:boolean marked-destroyed?
   ^HashSet pipes
   ^:unsynchronized-mutable ^:long last-time-cmds-processed
   ^:unsynchronized-mutable ^:int n-msgs-since-processed
   ^:unsynchronized-mutable ^:boolean awaiting-more?
   ^:unsynchronized-mutable ^Signaller reaper-signaller
   ^:unsynchronized-mutable ^:boolean terminating?
   ^:boolean thread-safe?
   ^ReentrantLock lock
   ^:unsynchronized-mutable ^clojure.lang.IFn destroy-hook]

  mbox/HasMailbox
  (get-mailbox [_] mailbox)

  IsSocketBase
  (thread-safe? [_] thread-safe?)
  (register-pipe! [_ pipe socket]
    (pipe/set-event-sink! pipe socket)
    (.add pipes pipe))
  (maybe-terminate-pipe! [_ pipe]
    (when terminating?
      (pipe/terminate! pipe false)))
  (stop! [this] (mbox/send! mailbox (cmd/new-cmd ::cmd/stop this)))
  (add-endpoint! [this addr endpoint pipe]
    (with-sock-lock this
      (set! endpoints (update endpoints addr
                              #(util/set-conj % (->EndpointWithPipe endpoint pipe))))))
  (terminate-endpoint! [this addr]
    (with-sock-lock this
      (let [endpoints' (get endpoints addr)]
        (set! endpoints (disj endpoints addr))
        (if (or (nil? endpoints') (empty? endpoints'))
          (error "ENOENT")
          (doseq [^EndpointWithPipe ep endpoints']
            (when (some? (.-pipe ep))
              (pipe/terminate! (.-pipe ep) true))
            (mbox/send! (mbox/get-mailbox ^mbox/HasMailbox (.-endpoint ep))
                        (cmd/new-cmd ::cmd/term (.-endpoint ep) #_linger ;; TODO
                                     )))))))
  (-process-commands! [_ timeout throttle?]
    (let [first-cmd (if (zero? timeout)
                      (mbox/recv! mailbox timeout)
                      (let [time (System/nanoTime)]
                        (or (and throttle?
                                 (and (<= last-time-cmds-processed time)
                                      (<= (- time last-time-cmds-processed) max-cmd-delay))
                                 ::return)
                            (do (when throttle?
                                  (set! last-time-cmds-processed time))
                                (mbox/recv! mailbox 0)))))]
      (loop [cmd first-cmd]
        (cmd/execute! cmd)
        (when-some [next-cmd (mbox/recv! mailbox 0)]
          (recur next-cmd)))))
  (remove-pipe! [_ pipe] (.remove pipes pipe))
  (init-reaper-signaller! [_]
    (util/with-lock lock
      (set! reaper-signaller (mbox/new-signaller))
      (mbox/add-signaller! mailbox reaper-signaller)
      (mbox/sig-send! reaper-signaller)
      (mbox/sig-handle reaper-signaller)))
  (close! [this after-hook]
    (set! alive? false)
    (with-sock-lock this
      (when thread-safe?
        (mbox/clear-signallers! mailbox))
      (after-hook)))
  (set-destroy-hook! [_ hook]
    (set! destroy-hook hook))
  (maybe-destroy! [_]
    (when marked-destroyed?
      (destroy-hook)
      (try (mbox/close! mailbox)
           (catch java.io.IOException _silent))
      (when (some? reaper-signaller)
        (try (mbox/sig-close! reaper-signaller)
             (catch java.io.IOException _silent)))))

  poller/HandlesEventIn
  (handle-in [this]
    (with-sock-lock this
      (when thread-safe?
        (mbox/sig-recv! reaper-signaller))
      (-process-commands! this 0 false))
    (maybe-destroy! this))

  Commandable
  (handle-cmd [this cmd data]
    (case cmd
      ::cmd/stop nil
      ::cmd/bind (attach-pipe! this ^pipe/Pipe data false)
      ::cmd/term
      (let [^:int linger data]
        (doseq [pipe pipes]
          (pipe/send-disconnect-msg! pipe)
          (pipe/terminate! pipe false))
        ;; (own/handle-term) ;; TODO
        (set! terminating? true)))))

(defn new-socket-base [id thread-safe?]
  (let [lock (ReentrantLock.)]
    (->SocketBase (if thread-safe?
                   (mbox/new-safe-mailbox lock (str "safe-socket-" id))
                   (mbox/new-mailbox (str "socket-" id)))
                  {} ;; endpoints
                  true ;; alive?
                  false ;; marked destroyed?
                  (HashSet.) ;; pipes
                  0 ;; last time commands processed
                  0 ;; n messages since last processed
                  false ;; awaiting-more?
                  nil ;; reaper signaller
                  false ;; terminating?
                  thread-safe?
                  (when thread-safe? (ReentrantLock.))
                  nil ;; destroy hook
                  )))

(defmacro with-attach-pipe! [socket socket-base pipe & body]
  `(do
     (register-pipe! ~socket-base ~pipe ~socket)
     ~@body
     (maybe-terminate-pipe! ~socket-base ~pipe)))

#_(comment
    (def ^:const event-flag-connected 1)
    (def ^:const event-flag-connect-delayed (bit-shift-left 1 1))
    (def ^:const event-flag-connect-retired (bit-shift-left 1 2))
    (def ^:const event-flag-listening (bit-shift-left 1 3))
    (def ^:const event-flag-bind-failed (bit-shift-left 1 4))
    (def ^:const event-flag-accepted (bit-shift-left 1 5))
    (def ^:const event-flag-accept-failed (bit-shift-left 1 6))
    (def ^:const event-flag-closed (bit-shift-left 1 7))
    (def ^:const event-flag-close-failed (bit-shift-left 1 8))
    (def ^:const event-flag-disconnected (bit-shift-left 1 9))
    (def ^:const event-flag-monitor-stopped (bit-shift-left 1 10))
    (def ^:const event-flag-handshake-protocol (bit-shift-left 1 15))
    (def ^:const event-flag-all (dec (enc/pow 2 16)))

  ;; monitor sockets are inproc only
    monitor-sock
    monitor-events-bitmask
    monitor-sync
    (-stop-monitor! [_]
                    (when (some? monitor-sock)
                      (when (not= 0 (bit-and monitor-events-bitmask event-flag-monitor-stopped))
                        (event-flag-monitor-stopped "" 0))
                      (close! monitor-sock)
                      (set! monitor-sock nil)
                      (set! monitor-events-bitmask 0))))
