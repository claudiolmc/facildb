/**
 * Copyright 2020 Claudio Montenegro Chaves - CMC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package br.tec.cmc.facildb.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

    public static final String PATTERN_DATE_HOUR = "yyyy-MM-dd HH:mm:ss";
    public static final String PATTERN_DATE_T_HOUR = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String PATTERN_DATE_HOUR_MILISECONDS = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String PATTERN_DATE_T_HOUR_MILISECONDS = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    public static final String PATTERN_DATE_ONLY = "yyyy-MM-dd";

    private DateUtil() {
        throw new IllegalStateException("Utility class");
    }
    
    public static String dateToString(Date dt, String pattern) {
        SimpleDateFormat sdt = new SimpleDateFormat(pattern);
        return sdt.format(dt);
    }

    public static Date stringToDate(String sDate) throws ParseException {
        return DateUtil.stringToDate(sDate, PATTERN_DATE_HOUR_MILISECONDS);
    }
        
    public static Date stringToDate(String sDate, String pattern) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        sDate = sDate.replace("T", " ");
        return sdf.parse(sDate);
    }
    
}
