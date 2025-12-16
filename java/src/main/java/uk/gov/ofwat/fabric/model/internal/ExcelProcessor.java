package uk.gov.ofwat.fabric.model.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelProcessor {

    public static void main(String[] args) {
        System.out.println("Starting ExcelProcessor");

        if (args.length != 2) {
            System.out.println("Usage: java ExcelProcessor <input.xlsx> <output.xlsx>");
            return;
        }

        String inputFile = args[0];
        String outputFile = args[1];
        System.out.println("Now have two args");
        System.out.println("             arg1: " + inputFile);
        System.out.println("             arg2: " + outputFile);
        
        try {
            Workbook workbook = readExcelInputFile(inputFile);
            workbook = calcWorkbook(workbook);
            writeToFile(workbook, outputFile);
        } catch (Exception e) {
            System.err.println("Error processing Excel file: " + e.getMessage());
            System.out.println("Error processing Excel file: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("Exiting ExcelProcessor");
        }
    }

    public static Workbook readExcelInputFile(String inputPath) throws FileNotFoundException, IOException {
        FileInputStream fis = new FileInputStream(new File(inputPath));
        Workbook workbook = new XSSFWorkbook(fis);
        System.out.println("read file " + inputPath);
        return workbook;
    }

   	public static Workbook calcWorkbook(Workbook workbook) {
		FormulaEvaluator formulaEvaluator = workbook.getCreationHelper().createFormulaEvaluator();
		formulaEvaluator.setIgnoreMissingWorkbooks(true);
        System.out.println("Clearing cached result values");
		formulaEvaluator.clearAllCachedResultValues();
        System.out.println("Evaluating formula");
		formulaEvaluator.evaluateAll();
        return workbook;
	}
	
	public static void writeToFile(Workbook workbook, String fileName) throws FileNotFoundException, IOException {
		FileOutputStream outputStream = new FileOutputStream(fileName); 
		workbook.write(outputStream);
		workbook.close();
		outputStream.close();
        System.out.println("written file " + fileName);
	}
}