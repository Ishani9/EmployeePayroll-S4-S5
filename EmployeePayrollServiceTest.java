package com.bl.jdbcassignment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.bl.jdbcassignment.EmployeePayrollService.IOService;

public class EmployeePayrollServiceTest {

	public static EmployeePayrollService employeePayrollService;
	public static List<EmployeePayrollData> employeePayrollData;
	
	@BeforeClass
	public static void setUp() {
		employeePayrollService = new EmployeePayrollService();
		employeePayrollData = employeePayrollService.readEmployeeData(IOService.DB_IO);
	}

	@Test
	public void givenEmployeePayrollDB_WhenRetrieved_ShouldMatchEmployeeCount() throws SQLException {
		assertEquals(4, employeePayrollData.size());
	}
	
	@Test
	public void givenNewSalaryForEmployee_WhenUpdated_ShouldSyncWithDb() throws SQLException
	{
		employeePayrollService.updateEmployeePayrollSalary("Bill", 200000.00);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Bill");
		assertEquals(4, employeePayrollData.size());
		assertTrue(result);
	}
	
	/**
	 * UC 5
	 * 
	 */
	@Test
	public void givenDateRange_WhenRetrieved_ShouldMatchEmployeeCount() {
		List<EmployeePayrollData> employeeByDateList = null;
		LocalDate start = LocalDate.of(2018, 3, 3);
		LocalDate end = LocalDate.of(2020, 6, 12);
		employeeByDateList = employeePayrollService.getEmployeeByDate(start, end);
		assertEquals(3, employeeByDateList.size());
	}
	
	/**
	 * UC 6
	 * 
	 */
	@Test
	public void givenEmployees_WhenRetrievedAverage_ShouldReturnComputedMap() {
		Map<String, Double> genderComputedMap = employeePayrollService.getEmployeeAverageByGender();
		assertTrue(genderComputedMap.get("M").equals(350000.0));
		assertTrue(genderComputedMap.get("F").equals(350000.0));
	}

	@Test
	public void givenEmployees_WhenRetrievedMaximumSalaryByGender_ShouldReturnComputedMap() {
		Map<String, Double> genderComputedMap = employeePayrollService.getEmployeeMaximumSalaryByGender();
		assertTrue(genderComputedMap.get("M").equals(500000.0));
		assertTrue(genderComputedMap.get("F").equals(400000.0));
	}

	@Test
	public void givenEmployees_WhenRetrievedMinimumSalaryByGender_ShouldReturnComputedMap() {
		Map<String, Double> genderComputedMap = employeePayrollService.getEmployeeMinimumSalaryByGender();
		assertTrue(genderComputedMap.get("M").equals(200000.0));
		assertTrue(genderComputedMap.get("F").equals(300000.0));
	}

	@Test
	public void givenEmployees_WhenRetrievedSumByGender_ShouldReturnComputedMap() {
		Map<String, Double> genderComputedMap = employeePayrollService.getEmployeeSumByGender();
		assertTrue(genderComputedMap.get("M").equals(700000.0));
		assertTrue(genderComputedMap.get("F").equals(700000.0));
	}

	@Test
	public void givenEmployees_WhenRetrievedCountByGender_ShouldReturnComputedMap() {
		Map<String, Double> genderComputedMap = employeePayrollService.getEmployeeCountByGender();
		assertTrue(genderComputedMap.get("M").equals(2.0));
		assertTrue(genderComputedMap.get("F").equals(2.0));
	}
	
	/** **************************
	 * UC 7
	 * 
	 */
	@Test
	public void givenNewEmployee_WhenAdded_ShouldSincWithDB() throws SQLException {
		employeePayrollService.addEmployeeToPayroll("Charlie", "M", 500000.0, LocalDate.now());
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Charlie");
		assertTrue(result);
	}
	
	/**
	 * UC 8
	 * 
	 */
	@Test
	public void givenEmployeeId_WhenDeleted_shouldDeleteCascadingly() {
		employeePayrollService.deleteEmployeeFromPayroll(2);
		employeePayrollData = employeePayrollService.readEmployeeData(IOService.DB_IO);
		assertEquals(4, employeePayrollData.size());
	}
	
	/**
	 * ER DIAGRAM UC
	 * 
	 */
	@Test
	public void givenEmployeeId_WhenRemoved_shouldReturnNumberOfActiveEmployees() {
		List<EmployeePayrollData> onlyActiveList = employeePayrollService.removeEmployeeFromPayroll(2);
		assertEquals(4, onlyActiveList.size());
	}
	
	/**
	 * THREADS UC 1
	 * 
	 */
	@Test
	public void given3Employees_WhenAddedToDB_ShouldMatchEmployeeCount() throws PayrollServiceDBException {
		EmployeePayrollData[] arrayOfEmp = { new EmployeePayrollData(0, "Jeff", "M", 100000.0, LocalDate.now(), Arrays.asList("Sales")),
				new EmployeePayrollData(0, "Billi", "F", 200000.0, LocalDate.now(), Arrays.asList("Marketing")),
				new EmployeePayrollData(0, "Anil", "M", 300000.0, LocalDate.now(), Arrays.asList("Sales")) };
		Instant start = Instant.now();
		employeePayrollService.addMultipleEmployeesToPayroll(Arrays.asList(arrayOfEmp));
		Instant end = Instant.now();
		System.out.println("Duration without Thread: " + Duration.between(start, end));
		employeePayrollData = employeePayrollService.readEmployeeData(IOService.DB_IO);
		assertEquals(4, employeePayrollData.size());
	}
	

}

