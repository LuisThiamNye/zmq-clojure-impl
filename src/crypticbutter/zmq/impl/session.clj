(ns crypticbutter.zmq.impl.session)

(defprotocol SessionBase
  (reset-state! [_])
  (attach-pipe! [_ pipe])
  (push-msg! [_ msg]))
