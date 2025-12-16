package uk.gov.ofwat.fabric.model.internal;

import java.util.Collection;

import org.apache.poi.ss.formula.eval.FunctionEval;


public class POISpreadsheetFunctions {
    public static void main(String[] args) {

        Collection<String> supportedFunctions = FunctionEval.getSupportedFunctionNames();
        System.out.println("=== SUPPORTED EXCEL FUNCTIONS (" + supportedFunctions.size() + ") ===");
        supportedFunctions.forEach(System.out::println);

        Collection<String> unsupportedFunctions = FunctionEval.getNotSupportedFunctionNames();
        System.out.println("\n=== UNSUPPORTED EXCEL FUNCTIONS (" + unsupportedFunctions.size() + ") ===");
        unsupportedFunctions.forEach(System.out::println);    
    }

}