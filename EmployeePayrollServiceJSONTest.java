package com.bl.jdbcassignment;

import static org.junit.Assert.assertEquals;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import com.bl.jdbcassignment.EmployeePayrollService.IOService;
import com.google.gson.Gson;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

public class EmployeePayrollServiceJSONTest {
	
	@Before
	public void setup() {
		RestAssured.baseURI = "http://localhost";
		RestAssured.port = 3000;
	}

	/**
	 * retrieving data from json server
	 * 
	 * @return
	 */
	private EmployeePayrollData[] getEmployeeList() {
		io.restassured.response.Response response = RestAssured.get("/employees");
		System.out.println("Employee payroll entries in JSONServer:\n" + response.asString());
		EmployeePayrollData[] arrayOfEmp = new Gson().fromJson(response.asString(), EmployeePayrollData[].class);
		return arrayOfEmp;
	}

	/**
	 * adding employee to json server
	 * 
	 * @param employee
	 * @return
	 */
	private io.restassured.response.Response addEmployeeToJsonServer(EmployeePayrollData employee) {
		String empJson = new Gson().toJson(employee);
		RequestSpecification request = RestAssured.given();
		request.header("Content-Type", "application/json");
		request.body(empJson);
		return request.post("/employees");
	}

	/**
	 * REST UC 1
	 * 
	 */
	@Test
	public void givenNewEmployee_WhenAdded_ShouldMatch201ResponseAndCount() {
		EmployeePayrollData[] arrayOfEmp = getEmployeeList();
		EmployeePayrollService employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmp));
		EmployeePayrollData employee = new EmployeePayrollData(5, "Ratan Tata", "M", 5000000.0, LocalDate.now());
		Response response = addEmployeeToJsonServer(employee);
		int statusCode = response.getStatusCode();
		assertEquals(201, statusCode);
		employee = new Gson().fromJson(response.asString(), EmployeePayrollData.class);
		employeePayrollService.addEmployeeToPayroll(employee);
		long count = employeePayrollService.countEntries(IOService.REST_IO);
		assertEquals(6, count);
	}
	
	/**
	 * REST UC 2
	 * 
	 */
	@Test
	public void givenMultipleNewEmployees_WhenAdded_ShouldMatch201ResponseAndCount() {
		EmployeePayrollData[] arrayOfEmp = getEmployeeList();
		EmployeePayrollService employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmp));
		EmployeePayrollData[] arrayOfEmployee = { new EmployeePayrollData(0, "Tiss", "F", 6000000.0, LocalDate.now()),
				new EmployeePayrollData(0, "Jill", "M", 7000000.0, LocalDate.now()),
				new EmployeePayrollData(0, "Billi", "F", 9000000.0, LocalDate.now()) };
		List<EmployeePayrollData> employeeList = Arrays.asList(arrayOfEmployee);
		employeeList.forEach(employee -> {
			Runnable task = () -> {
				Response response = addEmployeeToJsonServer(employee);
				int statusCode = response.getStatusCode();
				assertEquals(201, statusCode);
				EmployeePayrollData newEmployee = new Gson().fromJson(response.asString(), EmployeePayrollData.class);
				employeePayrollService.addEmployeeToPayroll(newEmployee);
			};
			Thread thread = new Thread(task, employee.name);
			thread.start();
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		long count = employeePayrollService.countEntries(IOService.REST_IO);
		assertEquals(9, count);
	}
	
	/**
	 * REST UC 3
	 * 
	 */
	@Test
	public void givenNewSalaryForEmployee_WhenUpdated_ShouldMatch200Request() {
		EmployeePayrollData[] arrayOfEmp = getEmployeeList();
		EmployeePayrollService employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmp));
		employeePayrollService.updateEmployeePayrollSalary("Jeff", 12000000.0);
		EmployeePayrollData employee = employeePayrollService.getEmployeePayrollData("Jeff");
		String empJson = new Gson().toJson(employee);
		RequestSpecification request = RestAssured.given();
		request.header("Content-Type", "application/json");
		request.body(empJson);
		Response response = request.put("/employees/" + employee.id);
		int statusCode = response.getStatusCode();
		assertEquals(200, statusCode);
	}
	
	/**
	 * REST UC 4
	 * 
	 */
	@Test
	public void givenEmployeeDataInJSONServer_WhenRetrieved_ShouldMatchTheCount() {
		EmployeePayrollData[] arrayOfEmp = getEmployeeList();
		EmployeePayrollService employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmp));
		long entries = employeePayrollService.countEntries(IOService.REST_IO);
		assertEquals(12,entries);
	}
	
	/**
	 * REST UC 5
	 * 
	 */
	@Test
	public void givenEmployeeToDelete_WhenDeleted_ShouldMatch200ResponseAndCount() {
		EmployeePayrollData[] arrayOfEmp = getEmployeeList();
		EmployeePayrollService employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmp));
		EmployeePayrollData employee = employeePayrollService.getEmployeePayrollData("Jeff");
		RequestSpecification request = RestAssured.given();
		request.header("Content-Type", "application/json");
		Response response = request.delete("/employees/" + employee.id);
		int statusCode = response.getStatusCode();
		assertEquals(200, statusCode);
		employeePayrollService.deleteEmployeeJSON(employee.name);
		long count = employeePayrollService.countEntries(IOService.REST_IO);
		assertEquals(11, count);
	}
}


