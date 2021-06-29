(ns crypticbutter.zmq.impl.io-thread)

(deftype IOThread
    [title
     mailbox
     poller]
  )

(defn new-io-thread [id]
  (->IoThread id
              ))
