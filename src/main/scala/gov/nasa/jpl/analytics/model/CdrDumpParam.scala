package gov.nasa.jpl.analytics.model

import gov.nasa.jpl.analytics.base.Loggable

/**
  * Created by karanjeetsingh on 10/20/16.
  */
class CdrDumpParam extends Loggable with Serializable {

  var docType: String = _
  var part: String = _
  var hashes: java.util.Map[java.lang.String, java.lang.String] = _

}
