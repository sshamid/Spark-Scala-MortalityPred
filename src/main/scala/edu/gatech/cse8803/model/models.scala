/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

import java.util.Date



case class NoteEvent(patientID: String, hadmID: String, charttime: Date, text: String)


case class ICUevent(ICUstayID: String, patientID: String, gender: String, flag: String,
                    intime: Date, outtime: Date, age: Double,
                    sapsi_first: Double, sapsi_min: Double, sapsi_max: Double,
                    sofa_first: Double, sofa_min: Double, sofa_max: Double)

