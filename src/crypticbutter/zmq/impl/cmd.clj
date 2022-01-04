(ns crypticbutter.zmq.impl.cmd)

::stop ;; terminate IO thread
::plug ;; register io object with io thread
::own ;; inform socket of new object
::attach ;; attach engine to session
::bind ;; session->socket to establish pipe
::activate-read ;; writer->reader thread: inform sleeping reader of new messages
::activate-write
::hiccup ;; after creating in-pipe
::pipe-term ;; reader->writer
::pipe-term-ack ;; acknowledge pipe-term
::term-req ;; io-object->socket requesting io-object shutdown
::term ;; socket-> shutdown io-object
::term-ack ;; io-object->socket acknowledge
::reap ;; transfer ownership of closed socket to reaper thread
::reaped ;; closed socket -> reaper - inform socket already deallocated
::inproc-connected
::done ;; reaper->term when all sockets deallocated
::cancel

(defprotocol Commandable
  (handle-cmd [_ cmd data]))

(defrecord Command [^clojure.lang.Keyword cmd
                    destination ;; Commandable
                    data])

(defn new-cmd
  ([cmd destination]
   (->Command cmd destination nil))
  ([cmd destination data]
   (->Command cmd destination data)))

(defn send-cmd!
  ([ctx cmd destination])
  ([ctx cmd destination data]))

(defn execute! [^Command cmd]
  (handle-cmd (:destination cmd) (:cmd cmd) (:data cmd)))
