package uk.gov.ofwat.fabric.model.internal;

import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.apache.spark.sql.SparkSession;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class SparkExcelProcessor {

    public static void main(String[] args) {
        System.out.println("Starting ExcelProcessor");

        SparkSession spark = SparkSession.builder()
            .appName("ExcelProcessorJarInSpark")
            .getOrCreate();
        System.out.println("Now have a spark session");

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
            spark.stop();
            System.out.println("Stopped spark and will now exit");
        }
    }

    public static Workbook readExcelInputFile(String inputPath) throws Exception {
        Configuration conf = new Configuration();
        // This will automatically use the cluster's identity for authentication
        FileSystem fs = FileSystem.get(new URI(inputPath), conf);
        InputStream fis  = fs.open(new Path(inputPath));
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
	
    public static void writeToFile(Workbook workbook, String fileName) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(fileName), conf);
        OutputStream outputStream =  fs.create(new Path(fileName), true); 
        workbook.write(outputStream);
        outputStream.close();
        System.out.println("written file " + fileName);
    }
}