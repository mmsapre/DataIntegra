package com.integration.em.detect;

import com.integration.em.datatypes.DataType;

import java.util.List;

public class Classifier {

    public DataType classify(List<String> colFeatures) {

        DataType colDatatype = null;

        if (colFeatures.get(2).equals("a")
                && Double.parseDouble(colFeatures.get(5)) > 4.500
                && Boolean.parseBoolean(colFeatures.get(7)) == false
                && Double.parseDouble(colFeatures.get(0)) > 0.685)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("a")
                && Double.parseDouble(colFeatures.get(5)) > 4.500
                && Boolean.parseBoolean(colFeatures.get(7)) == false
                && Double.parseDouble(colFeatures.get(0)) <= 0.685)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("a")
                && Double.parseDouble(colFeatures.get(5)) > 4.500
                && Boolean.parseBoolean(colFeatures.get(7)) == true)
            colDatatype = DataType.bool;
        else if (colFeatures.get(2).equals("a")
                && Double.parseDouble(colFeatures.get(5)) <= 4.500
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(0)) > 0.700
                && Double.parseDouble(colFeatures.get(0)) > 0.805
                && Boolean.parseBoolean(colFeatures.get(7)) == false
                && Double.parseDouble(colFeatures.get(5)) > 0.500)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("a")
                && Double.parseDouble(colFeatures.get(5)) <= 4.500
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(0)) > 0.700
                && Double.parseDouble(colFeatures.get(0)) > 0.805
                && Boolean.parseBoolean(colFeatures.get(7)) == false
                && Double.parseDouble(colFeatures.get(5)) <= 0.500)
            colDatatype = DataType.bool;
        else if (colFeatures.get(2).equals("a")
                && Double.parseDouble(colFeatures.get(5)) <= 4.500
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(0)) > 0.700
                && Double.parseDouble(colFeatures.get(0)) > 0.805
                && Boolean.parseBoolean(colFeatures.get(7)) == true)
            colDatatype = DataType.bool;
        else if (colFeatures.get(2).equals("a")
                && Double.parseDouble(colFeatures.get(5)) <= 4.500
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(0)) > 0.700
                && Double.parseDouble(colFeatures.get(0)) <= 0.805)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("a")
                && Double.parseDouble(colFeatures.get(5)) <= 4.500
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(0)) <= 0.700)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("a")
                && Double.parseDouble(colFeatures.get(5)) <= 4.500
                && Boolean.parseBoolean(colFeatures.get(3)) == true)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("a_a"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("ad")
                && Double.parseDouble(colFeatures.get(1)) > 0.020)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("ad")
                && Double.parseDouble(colFeatures.get(1)) <= 0.020)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("ada"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("adsap"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apa")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(5)) > 6)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apa")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(5)) <= 6
                && Double.parseDouble(colFeatures.get(0)) > 0.900
                && Double.parseDouble(colFeatures.get(0)) > 0.995)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apa")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(5)) <= 6
                && Double.parseDouble(colFeatures.get(0)) > 0.900
                && Double.parseDouble(colFeatures.get(0)) <= 0.995)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("apa")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(5)) <= 6
                && Double.parseDouble(colFeatures.get(0)) <= 0.900)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apa")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == true)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apa")
                && Boolean.parseBoolean(colFeatures.get(6)) == true)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apaapdad"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apap"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapad"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapap"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapapad"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapapap"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapapapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapapapadpdadpa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapapapapad"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapapapapapapapapapapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2)
                .equals("apapapapapapdpdpdpdpdpapapapapapapdpsa"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("apapapapapapdpsa"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("apapapapapapdspapapap"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapapapdpsa"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("apapd"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apapspd"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apd")
                && Double.parseDouble(colFeatures.get(0)) > 0.430)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("apd")
                && Double.parseDouble(colFeatures.get(0)) <= 0.430)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("apdapdasapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apdapdpdpdpa"))
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("apdp"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apdpd")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == false)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apdpd")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == true)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("apdpd")
                && Boolean.parseBoolean(colFeatures.get(6)) == true)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("apdpdpd"))
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("apsapapapapapapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apsapapapapapapapap"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("apspa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("as"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("asa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("asap"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("asapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("asapapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("asapapasa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("asapdpd"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("asds"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(5)) > 0.500)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(5)) <= 0.500
                && Double.parseDouble(colFeatures.get(0)) > 0.310)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(5)) <= 0.500
                && Double.parseDouble(colFeatures.get(0)) <= 0.310)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) > 0.450
                && Double.parseDouble(colFeatures.get(0)) > 0.825)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) > 0.450
                && Double.parseDouble(colFeatures.get(0)) <= 0.825)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) <= 0.450
                && Boolean.parseBoolean(colFeatures.get(7)) == false
                && Double.parseDouble(colFeatures.get(0)) > -0.500
                && Double.parseDouble(colFeatures.get(1)) > 0.085
                && Double.parseDouble(colFeatures.get(0)) > 0.070)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) <= 0.450
                && Boolean.parseBoolean(colFeatures.get(7)) == false
                && Double.parseDouble(colFeatures.get(0)) > -0.500
                && Double.parseDouble(colFeatures.get(1)) > 0.085
                && Double.parseDouble(colFeatures.get(0)) <= 0.070)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) <= 0.450
                && Boolean.parseBoolean(colFeatures.get(7)) == false
                && Double.parseDouble(colFeatures.get(0)) > -0.500
                && Double.parseDouble(colFeatures.get(1)) <= 0.085)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) <= 0.450
                && Boolean.parseBoolean(colFeatures.get(7)) == false
                && Double.parseDouble(colFeatures.get(0)) <= -0.500)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) <= 0.450
                && Boolean.parseBoolean(colFeatures.get(7)) == true)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(0)) > -0.500)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(0)) <= -0.500)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) > 0.185
                && Double.parseDouble(colFeatures.get(0)) > 0.335
                && Double.parseDouble(colFeatures.get(0)) > 0.515)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) > 0.185
                && Double.parseDouble(colFeatures.get(0)) > 0.335
                && Double.parseDouble(colFeatures.get(0)) <= 0.515)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) > 0.185
                && Double.parseDouble(colFeatures.get(0)) <= 0.335)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) <= 0.185
                && Double.parseDouble(colFeatures.get(5)) > 3.500)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("d")
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Double.parseDouble(colFeatures.get(0)) <= 0.185
                && Double.parseDouble(colFeatures.get(5)) <= 3.500)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("da"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dp"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpa")
                && Double.parseDouble(colFeatures.get(0)) > 0.515
                && Double.parseDouble(colFeatures.get(0)) > 0.610
                && Double.parseDouble(colFeatures.get(0)) > 0.740)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpa")
                && Double.parseDouble(colFeatures.get(0)) > 0.515
                && Double.parseDouble(colFeatures.get(0)) > 0.610
                && Double.parseDouble(colFeatures.get(0)) <= 0.740)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dpa")
                && Double.parseDouble(colFeatures.get(0)) > 0.515
                && Double.parseDouble(colFeatures.get(0)) <= 0.610)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpa")
                && Double.parseDouble(colFeatures.get(0)) <= 0.515)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpap"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpapa")
                && Double.parseDouble(colFeatures.get(0)) > 0.745)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpapa")
                && Double.parseDouble(colFeatures.get(0)) <= 0.745)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpapapapapd"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dpapapd"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dpapd")
                && Boolean.parseBoolean(colFeatures.get(6)) == false)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpapd")
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Double.parseDouble(colFeatures.get(0)) > 0.420)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dpapd")
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Double.parseDouble(colFeatures.get(0)) <= 0.420)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpapdpa")
                && Double.parseDouble(colFeatures.get(0)) > 0.490)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dpapdpa")
                && Double.parseDouble(colFeatures.get(0)) <= 0.490)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpapdpd"))
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpasa"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(0)) > -0.500
                && Double.parseDouble(colFeatures.get(1)) > 0.155)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(0)) > -0.500
                && Double.parseDouble(colFeatures.get(1)) <= 0.155
                && Double.parseDouble(colFeatures.get(0)) > 0.195
                && Double.parseDouble(colFeatures.get(1)) > 0.105)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(0)) > -0.500
                && Double.parseDouble(colFeatures.get(1)) <= 0.155
                && Double.parseDouble(colFeatures.get(0)) > 0.195
                && Double.parseDouble(colFeatures.get(1)) <= 0.105)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(0)) > -0.500
                && Double.parseDouble(colFeatures.get(1)) <= 0.155
                && Double.parseDouble(colFeatures.get(0)) <= 0.195
                && Double.parseDouble(colFeatures.get(1)) > 0.140)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(0)) > -0.500
                && Double.parseDouble(colFeatures.get(1)) <= 0.155
                && Double.parseDouble(colFeatures.get(0)) <= 0.195
                && Double.parseDouble(colFeatures.get(1)) <= 0.140
                && Double.parseDouble(colFeatures.get(5)) > 5.500)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(0)) > -0.500
                && Double.parseDouble(colFeatures.get(1)) <= 0.155
                && Double.parseDouble(colFeatures.get(0)) <= 0.195
                && Double.parseDouble(colFeatures.get(1)) <= 0.140
                && Double.parseDouble(colFeatures.get(5)) <= 5.500)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(0)) <= -0.500
                && Double.parseDouble(colFeatures.get(5)) > 4.500)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(0)) <= -0.500
                && Double.parseDouble(colFeatures.get(5)) <= 4.500)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Double.parseDouble(colFeatures.get(1)) > 0.125)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Double.parseDouble(colFeatures.get(1)) <= 0.125
                && Double.parseDouble(colFeatures.get(5)) > 9)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Boolean.parseBoolean(colFeatures.get(6)) == true
                && Double.parseDouble(colFeatures.get(1)) <= 0.125
                && Double.parseDouble(colFeatures.get(5)) <= 9)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(1)) > 0.325)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(1)) <= 0.325
                && Double.parseDouble(colFeatures.get(0)) > 0.300)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(1)) <= 0.325
                && Double.parseDouble(colFeatures.get(0)) <= 0.300
                && Double.parseDouble(colFeatures.get(5)) > 2.500)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Double.parseDouble(colFeatures.get(1)) <= 0.325
                && Double.parseDouble(colFeatures.get(0)) <= 0.300
                && Double.parseDouble(colFeatures.get(5)) <= 2.500)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpd")
                && Boolean.parseBoolean(colFeatures.get(3)) == true
                && Boolean.parseBoolean(colFeatures.get(6)) == true)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpda")
                && Double.parseDouble(colFeatures.get(0)) > 0.320)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpda")
                && Double.parseDouble(colFeatures.get(0)) <= 0.320)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpdpa")
                && Double.parseDouble(colFeatures.get(0)) > 0.350)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpdpa")
                && Double.parseDouble(colFeatures.get(0)) <= 0.350)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpdpapad"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpdpapap"))
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpdpapd"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpdpasapsap"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpdpd")
                && Double.parseDouble(colFeatures.get(1)) > 0.235)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpdpd")
                && Double.parseDouble(colFeatures.get(1)) <= 0.235
                && Boolean.parseBoolean(colFeatures.get(6)) == false)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpdpd")
                && Double.parseDouble(colFeatures.get(1)) <= 0.235
                && Boolean.parseBoolean(colFeatures.get(6)) == true)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpdpdapdpd"))
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpdpdpa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dpdpdpd")
                && Boolean.parseBoolean(colFeatures.get(6)) == false)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpdpdpd")
                && Boolean.parseBoolean(colFeatures.get(6)) == true)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dpdpdpdpd")
                && Boolean.parseBoolean(colFeatures.get(6)) == false)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dpdpdpdpd")
                && Boolean.parseBoolean(colFeatures.get(6)) == true)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dpdpsapsapsapsap"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpds"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dpdsap"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpdsapa"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dpsa"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("ds"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dsapd"))
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dsapdpa"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("dsd")
                && Boolean.parseBoolean(colFeatures.get(6)) == false)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("dsd")
                && Boolean.parseBoolean(colFeatures.get(6)) == true)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("dsdpapdp"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dsdpasdpsdpdsdpa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("dsdsd"))
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("pd"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("pdasap"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("pdpa"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("pdpd")
                && Double.parseDouble(colFeatures.get(1)) > 0.345)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("pdpd")
                && Double.parseDouble(colFeatures.get(1)) <= 0.345)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("pdpdpd"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("pdpds"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("sap")
                && Double.parseDouble(colFeatures.get(0)) > 0.660
                && Double.parseDouble(colFeatures.get(5)) > 5.500)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("sap")
                && Double.parseDouble(colFeatures.get(0)) > 0.660
                && Double.parseDouble(colFeatures.get(5)) <= 5.500
                && Double.parseDouble(colFeatures.get(0)) > 0.705)
            colDatatype = DataType.bool;
        else if (colFeatures.get(2).equals("sap")
                && Double.parseDouble(colFeatures.get(0)) > 0.660
                && Double.parseDouble(colFeatures.get(5)) <= 5.500
                && Double.parseDouble(colFeatures.get(0)) <= 0.705)
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("sap")
                && Double.parseDouble(colFeatures.get(0)) <= 0.660)
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("sapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("sapapasdpapapapapdsdpdpds"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("sapd"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("sapdpapasap"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sapdpd"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sapdpds"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("sapdpdspdpap"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("sapdsap"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sapsapa"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("sapsapdpd"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("sapsas"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("sapsd"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sasas"))
            colDatatype = DataType.string;
        else if (colFeatures.get(2).equals("sd")
                && Double.parseDouble(colFeatures.get(1)) > 0.055)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("sd")
                && Double.parseDouble(colFeatures.get(1)) <= 0.055
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(0)) > 0.025)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sd")
                && Double.parseDouble(colFeatures.get(1)) <= 0.055
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == false
                && Double.parseDouble(colFeatures.get(0)) <= 0.025)
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("sd")
                && Double.parseDouble(colFeatures.get(1)) <= 0.055
                && Boolean.parseBoolean(colFeatures.get(6)) == false
                && Boolean.parseBoolean(colFeatures.get(3)) == true)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sd")
                && Double.parseDouble(colFeatures.get(1)) <= 0.055
                && Boolean.parseBoolean(colFeatures.get(6)) == true)
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sda"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sdp"))
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("sdpa"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sdpas"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sdpd"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sdpda"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("sdpdpa"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sdpdpd"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("sdpdpdpa"))
            colDatatype = DataType.unit;
        else if (colFeatures.get(2).equals("sdpdpdsapdpds"))
            colDatatype = DataType.date;
        else if (colFeatures.get(2).equals("sdpsdsds"))
            colDatatype = DataType.number;
        else if (colFeatures.get(2).equals("spdpda"))
            colDatatype = DataType.unit;
        else
            colDatatype = DataType.string;

        return colDatatype;
    }
}
