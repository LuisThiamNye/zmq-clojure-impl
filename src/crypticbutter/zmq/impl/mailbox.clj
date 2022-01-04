(ns crypticbutter.zmq.impl.mailbox
  (:require
   [farolero.core :refer [error]]
   [crypticbutter.zmq.impl.util :as util]
   [crypticbutter.zmq.impl.pipe.ypipe :as ypipe])
  (:import
   (java.util ArrayList)
   (java.nio.channels Selector Pipe$SinkChannel Pipe$SourceChannel)
   (java.nio ByteBuffer)
   (java.util.concurrent.locks Condition ReentrantLock)
   (crypticbutter.zmq.impl.pipe.ypipe YPipe)))

(defprotocol SignallerProtocol
  (sig-handle [_])
  (sig-send! [_])
  (sig-recv! [_])
  (sig-wait! [_ timeout] "To wait forever, specify timeout of `-1`")
  (sig-close! [_]))

(defn endure-interrupt [f]
  (loop []
    (let [r (try (f)
                 (catch java.nio.channels.ClosedByInterruptException _
                   ::interrupted))]
      (if (= ::interrupted r)
        (do (.interrupt (Thread/currentThread))
            (recur))
        r))))

(deftype Signaller
  [^Pipe$SinkChannel write-chan
   ^Pipe$SourceChannel read-chan
   ^Selector selector
   ^ByteBuffer write-dummy
   ^ByteBuffer read-dummy
   ^clojure.lang.Atom *write-cursor
   ^:unsynchronized-mutable ^:long read-cursor]

  SignallerProtocol
  (sig-handle [_] read-chan)
  (sig-send! [this]
    (.clear write-dummy)
    (let [bytes-written (endure-interrupt #(.write write-chan write-dummy))]
      (if (zero? bytes-written)
        (recur)
        (swap! *write-cursor inc))))
  (sig-recv! [this]
    (try (loop []
           (.clear read-dummy)
           (let [bytes-read (endure-interrupt #(.read read-chan read-dummy))]
             (if (zero? bytes-read)
               (recur)
               (set! read-cursor (inc read-cursor)))))
         (catch java.nio.channels.ClosedChannelException _
           (error "EINTR"))))
  (sig-wait! [_ timeout]
    (if (Thread/interrupted)
      (error "EINTR")
      (let [updated-key-count (when (<= @*write-cursor read-cursor)
                                (try (cond
                                       (zero? timeout) (error "EAGAIN")
                                       (neg? timeout) (.select selector 0)
                                       :else (.select selector timeout))
                                     (catch java.nio.channels.ClosedSelectorException _
                                       (error "EINTR"))))
            no-updated-keys? (zero? updated-key-count)]
        (cond
          (or (Thread/interrupted)
              (and no-updated-keys?
                   (<= timeout 0)
                   (not (.isEmpty (.keys selector)))))
          (error "EINTR")
          no-updated-keys? (error "EAGAIN"))
        (.clear (.selectedKeys selector)))))
  (sig-close! [_]
    (loop [exception nil
           tasks [#(.close read-chan)
                  #(.close write-chan)
                  #(.close selector)]]
      (if (nil? tasks)
        (when (some? exception)
          (throw exception))
        (recur (try (endure-interrupt (first tasks))
                    exception
                    (catch java.io.IOException e
                      (some->> exception (.addSuppressed e))
                      e))
               (next tasks))))))

(defn new-signaller []
  (let [pipe (java.nio.channels.Pipe/open)
        selector (Selector/open)]
    (->Signaller (doto (.sink pipe) ;; write chan
                   (.configureBlocking false))
                 (doto (.source pipe) ;; read chan
                   (.configureBlocking false)
                   (.register selector java.nio.channels.SelectionKey/OP_READ))
                 selector
                 (ByteBuffer/allocate 1)
                 (ByteBuffer/allocate 1)
                 (atom 0) ;; write cursor
                 0 ;; read cursor
                 )))

(defprotocol MailboxProtocol
  (send! [_ cmd])
  (recv! [_ timeout] "-1 timeout to block indefinitely.")
  (close! [_]))

(defprotocol UnsafeMailboxProtocol
  (handle [_]))

(deftype Mailbox
  [^:string title
   ^YPipe cmd-pipe
   ^Signaller signaller
   ^Object sendsync
   ^:unsynchronized-mutable ^:boolean active?]

  UnsafeMailboxProtocol
  (handle [_] (sig-handle signaller))

  MailboxProtocol
  (send! [_ cmd]
    (when-not (locking sendsync
                (ypipe/write! cmd-pipe cmd false)
                (ypipe/flush! cmd-pipe))
      ;; Reader is sleeping
      (sig-send! signaller)))
  (recv! [_ timeout]
    (or (and active? (ypipe/read! cmd-pipe))
        (do
          (when active? (set! active? false))
          (sig-wait! signaller timeout)
          (sig-recv! signaller)
          (set! active? true)
          (ypipe/read! cmd-pipe))))
  (close! [_]
    ;; wait for threads in the send! fn to get the 🦆 out of there
    (locking sendsync)
    (sig-close! signaller)))

(def ^:const cmd-pipe-granularity 16)

(defn new-mailbox [title]
  (->Mailbox title
             (ypipe/new-ypipe cmd-pipe-granularity false)
             (new-signaller)
             (Object.)
             false))

(defprotocol SafeMailboxProtocol
  (add-signaller! [_ signaller])
  (remove-signaller! [_ signaller])
  (clear-signallers! [_]))

(deftype SafeMailbox
  [^:string title
   ^YPipe cmd-pipe
   ^ArrayList signallers
   ^ReentrantLock lock
   ^Condition condition]

  SafeMailboxProtocol
  (add-signaller! [_ signaller]
    (.add signallers signaller))
  (remove-signaller! [_ signaller]
    (.remove signallers signaller))
  (clear-signallers! [_]
    (.clear signallers))

  MailboxProtocol
  (send! [_ cmd]
    (util/with-lock lock
      (ypipe/write! cmd-pipe cmd false)
      (let [reader-sleeping? (ypipe/flush! cmd-pipe)]
        (when reader-sleeping?
          (.signalAll condition)
          (doseq [signaller signallers]
            (sig-send! signaller))))))
  (recv! [_ timeout]
    (or (ypipe/read! cmd-pipe)
        (do
          (if (zero? timeout)
            (doto lock .unlock .lock)
            (try
              (if (= -1 timeout)
                (.await condition)
                (.await condition timeout java.util.concurrent.TimeUnit/MILLISECONDS))
              (catch java.lang.InterruptedException _
                (error "EINTR"))))
          (or (ypipe/read! cmd-pipe)
              (error "EAGAIN")))))
  (close! [_]
    ;; Wait for threads using `send!`
    (doto lock .lock .unlock)))

(defn new-safe-mailbox [title ^ReentrantLock lock]
  (->SafeMailbox title
                 (ypipe/new-ypipe cmd-pipe-granularity false)
                 (ArrayList. 10)
                 lock
                 (.newCondition lock)))

(defprotocol HasMailbox
  (get-mailbox [_]))
