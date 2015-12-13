package org.apache.flink.quickstart

/**
 * Created by nirmalya on 13/12/15.
 */
case class IncomingDataUnit (
                              sensorUUID: String, radiationLevel: Int,photoSensor: Float,
                              humidity: Float,timeStamp: Long, ambientTemperature: Float)
  extends Serializable {


  /*SCHEMA (found here: https://www.pubnub.com/developers/data-streams/sensor-network)
  { "timestamp": 1447852019,"ambient_temperature": 19.46,"photosensor": 778.44,"radiation_level": 202,"sensor_uuid": "probe-ab73311f","humidity": 80.0990}
  */
}
