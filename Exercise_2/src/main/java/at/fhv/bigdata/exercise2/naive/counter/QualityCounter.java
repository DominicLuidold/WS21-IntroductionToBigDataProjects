package at.fhv.bigdata.exercise2.naive.counter;

/**
 * 0 = Passed gross limits check
 * 1 = Passed all quality control checks
 * 2 = Suspect
 * 3 = Erroneous
 * 4 = Passed gross limits check, data originate from an NCEI data source
 * 5 = Passed all quality control checks, data originate from an NCEI data source
 * 6 = Suspect, data originate from an NCEI data source
 * 7 = Erroneous, data originate from an NCEIdata source
 * 9 = Passed gross limits check if element is present
 * A = Data value flagged as suspect, but accepted as a good value
 * C = Temperature and dew point received from Automated Weather Observing System (AWOS)
 *  are reported in whole degrees Celsius.
 *  Automated QC flags these values, but they are accepted as valid.
 * I = Data value not originally in data, but inserted by validator
 * M = Manual changes made to value based on information provided by NWS or FAA
 * P = Data value not originally flagged as suspect, but replaced by validator
 * R = Data value replaced with value computed by NCEI software
 * U = Data value replaced with edited value
 *
 * @author Mariam Cordero Jimenez
 * @author Dominic Luidold
 */
public enum QualityCounter {
    QUALITY_0,
    QUALITY_1,
    QUALITY_2,
    QUALITY_3,
    QUALITY_4,
    QUALITY_5,
    QUALITY_6,
    QUALITY_7,
    QUALITY_8,
    QUALITY_9,
    QUALITY_A,
    QUALITY_C,
    QUALITY_I,
    QUALITY_M,
    QUALITY_P,
    QUALITY_R,
    QUALITY_U;
}
