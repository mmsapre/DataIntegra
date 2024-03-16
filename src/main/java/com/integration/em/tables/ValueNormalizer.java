package com.integration.em.tables;

import com.integration.em.datatypes.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.text.ParseException;
@Slf4j
public class ValueNormalizer {

    public Object normalize(String value, DataType type, UnitCategory unitcategory) {
        Object normalizedValue = null;
        
        if(value!=null) {
	        try {
		        switch (type) {
		            case string:
		            	normalizedValue = value;
		                break;
		            case date:
		            	normalizedValue = JavaDateTime.parse(value);
		                break;
					case number:
		                //TODO: how to handle numbers with commas (German style)
		      
		            	Quantity quantity = UnitCategoryParser.checkQuantity(value);
		                Unit unit = UnitCategoryParser.checkUnit(value, unitcategory);
		                
		                normalizedValue = UnitCategoryParser.transform(value, unit, quantity); 
		                
		                break;
		            case bool:
		            	normalizedValue = Boolean.parseBoolean(value);
		                break;

		            case link:
		            	normalizedValue = value;
		            default:
		                break;
		        }
	        } catch(ParseException e) {
	        	log.trace("ParseException for " + type.name() + " value: " + value);
	        		//e.printStackTrace();
	        }
        }
        
        return normalizedValue;
    }
}
