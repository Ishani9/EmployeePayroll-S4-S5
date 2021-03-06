package com.bl.jdbcassignment;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EmployeePayrollService {
	
	private static final Logger LOG = LogManager.getLogger(EmployeePayrollService.class);
	
	public enum IOService {
		CONSOLE_IO, FILE_IO, DB_IO, REST_IO
	};

	public List<EmployeePayrollData> employeePayrollList;
	private EmployeePayrollDBService employeePayrollDBService;

	/**
	 * Default Constructor for caching employeePayrollDBService object 
	 */
	public EmployeePayrollService() {
		employeePayrollDBService = EmployeePayrollDBService.getInstance();
	}
	
	public EmployeePayrollService(List<EmployeePayrollData> empList) {
		this();
		employeePayrollList = empList;
	}

	/**
	 * returns employeePayrollData object given name of employee
	 * 
	 * @param name
	 * @return
	 */
	public EmployeePayrollData getEmployeePayrollData(String name) {
		EmployeePayrollData employeePayrollData = this.employeePayrollList.stream()
				.filter(employeePayrollDataItem -> employeePayrollDataItem.name.equals(name)).findFirst().orElse(null);
		return employeePayrollData;
	}
	
	/**
	 * reads employee data from database and returns list of employee payroll data
	 * 
	 * @param ioService
	 * @return
	 */
	public List<EmployeePayrollData> readEmployeeData(IOService ioService) {
		try {
			if (ioService.equals(IOService.DB_IO)) {
				this.employeePayrollList = employeePayrollDBService.readData();
			}
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
		return this.employeePayrollList;
	}

	/**
	 * given name and updated salary of employee updates in the database
	 * 
	 * @param name
	 * @param salary
	 */
	public void updateEmployeePayrollSalary(String name, double salary) {
		int result = 0;
		try {
			result = employeePayrollDBService.updateEmployeeData(name, salary);
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
		if (result == 0) {
			return;
		}
		EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
		if (employeePayrollData != null) {
			employeePayrollData.salary = salary;
		}
	}

	/**
	 * checks if record matches with the updated record 
	 * 
	 * @param name
	 * @return
	 */
	public boolean checkEmployeePayrollInSyncWithDB(String name) {
		List<EmployeePayrollData> employeePayrollDataList = null;
		try {
			employeePayrollDataList = employeePayrollDBService.getEmployeePayrollData(name);
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
		return employeePayrollDataList.get(0).equals(getEmployeePayrollData(name));
	}
	
	/**
	 * UC 5
	 * 
	 */
	public List<EmployeePayrollData> getEmployeeByDate(LocalDate start, LocalDate end) {
		List<EmployeePayrollData> employeeByDateList = null;
		try {
			employeeByDateList = employeePayrollDBService.readDataForGivenDateRange(start, end);
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
		return employeeByDateList;
	}
	
	/**
	 * UC 6
	 * 
	 * returns map of gender and average salary
	 * 
	 * @return
	 */
	public Map<String, Double> getEmployeeAverageByGender() {
		Map<String, Double> genderComputedMap = new HashMap<>();
		try {
			genderComputedMap = employeePayrollDBService.getEmployeesByFunction("AVG");
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
		return genderComputedMap;
	}

	/**
	 * returns map of gender and sum of salaries
	 * 
	 * @return
	 */
	public Map<String, Double> getEmployeeSumByGender() {
		Map<String, Double> genderComputedMap = new HashMap<>();
		try {
			genderComputedMap = employeePayrollDBService.getEmployeesByFunction("SUM");
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
		return genderComputedMap;
	}

	/**
	 * returns map of gender and max salary
	 * 
	 * @return
	 */
	public Map<String, Double> getEmployeeMaximumSalaryByGender() {
		Map<String, Double> genderComputedMap = new HashMap<>();
		try {
			genderComputedMap = employeePayrollDBService.getEmployeesByFunction("MAX");
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
		return genderComputedMap;
	}

	/**
	 * returns map of gender and min salary
	 * 
	 * @return
	 */
	public Map<String, Double> getEmployeeMinimumSalaryByGender() {
		Map<String, Double> genderComputedMap = new HashMap<>();
		try {
			genderComputedMap = employeePayrollDBService.getEmployeesByFunction("MIN");
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
		return genderComputedMap;
	}

	/**
	 * returns map of gender and number of employees
	 * 
	 * @return
	 */
	public Map<String, Double> getEmployeeCountByGender() {
		Map<String, Double> genderComputedMap = new HashMap<>();
		try {
			genderComputedMap = employeePayrollDBService.getEmployeesByFunction("COUNT");
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
		return genderComputedMap;
	}
	
	/**=========================
	 * UC 7
	 * 
	 * adds employee details to database
	 * 
	 * @param name
	 * @param gender
	 * @param salary
	 * @param date
	 * @throws SQLException 
	 */
	public void addEmployeeToPayroll(String name, String gender, double salary, LocalDate date) throws SQLException {
		try {
			employeePayrollDBService.addEmployeeToPayrollUC9(name, gender, salary, date);
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
	}
	
	public void addEmployeeToPayroll(String name, String gender, double salary, LocalDate date,
			List<String> departments) {
		try {
			employeePayrollDBService.addEmployeeToPayroll(name, gender, salary, date, departments);
		} catch (PayrollServiceDBException | SQLException exception) {
			System.out.println(exception.getMessage());
		}
	}
	
	/**
	 * UC 8
	 * 
	 * deletes employee record from database
	 * 
	 * @param id
	 */
	public void deleteEmployeeFromPayroll(int id) {
		try {
			employeePayrollDBService.deleteEmployeeFromPayroll(id);
		} catch (PayrollServiceDBException exception) {
			System.out.println(exception.getMessage());
		}
	}

	/**
	 * UC 12
	 * 
	 * returns list of active employees
	 * 
	 * @param id
	 * @return
	 */
	public List<EmployeePayrollData> removeEmployeeFromPayroll(int id) {
		List<EmployeePayrollData> onlyActiveList = null;
		try {
			onlyActiveList = employeePayrollDBService.removeEmployeeFromCompany(id);
		} catch (PayrollServiceDBException e) {
			System.out.println(e.getMessage());
		}
		return onlyActiveList;
	}
	
	/**
	 * THREADS UC 1
	 * 
	 * @param employeeDataList
	 */
	public void addMultipleEmployeesToPayroll(List<EmployeePayrollData> employeeDataList) {
		employeeDataList.forEach(employee -> {
			System.out.println("Employee Being added: "+employee.name);
			try {
				employeePayrollDBService.addEmployeeToPayroll(employee.name,employee.gender,employee.salary,employee.startDate,employee.departments);
			} catch (SQLException | PayrollServiceDBException e) {
				e.printStackTrace();
			}
		System.out.println("Employee added: "+employee.name);
		});
		System.out.println(this.employeePayrollList);
	}
	
	/**
	 * THREADS UC 2
	 * 
	 * adding multiple employees to payroll with thread
	 * 
	 * @param employeeDataList
	 */
	public void addMultipleEmployeesToPayrollWithThreads(List<EmployeePayrollData> employeeDataList) {
		Map<Integer, Boolean> employeeAdditionStatus = new HashMap<Integer, Boolean>();
		employeeDataList.forEach(employee -> {
			Runnable task = () -> {
				employeeAdditionStatus.put(employee.hashCode(), false);
				//System.out.println("Employee Being Added: " + Thread.currentThread().getName());
				LOG.info("Employee Being Added: " + Thread.currentThread().getName());
				try {
					employeePayrollDBService.addEmployeeToPayroll(employee.name, employee.gender, employee.salary,
							employee.startDate, employee.departments);
				} catch (SQLException | PayrollServiceDBException e) {
					System.out.println(e.getMessage());
				}
				employeeAdditionStatus.put(employee.hashCode(), true);
				//System.out.println("Employee Added: " + Thread.currentThread().getName());
				LOG.info("Employee Added: " + Thread.currentThread().getName());
			};
			Thread thread = new Thread(task, employee.name);
			thread.start();
		});
		while (employeeAdditionStatus.containsValue(false)) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				System.out.println(e.getMessage());
			}
		}
	}
	
	/**
	 * Threads UC 6
	 * 
	 * updates multiple rows in database
	 * 
	 * @param newSalaryMap
	 */
	public void updateMultipleSalaries(Map<String, Double> newSalaryMap) {
		Map<Integer, Boolean> employeeAdditionStatus = new HashMap<Integer, Boolean>();
		newSalaryMap.forEach((k, v) -> {
			Runnable task = () -> {
				employeeAdditionStatus.put(k.hashCode(), false);
				LOG.info("Employee Being updated : " + Thread.currentThread().getName());
				//System.out.println("Employee Being updated : " + Thread.currentThread().getName());
				this.updateEmployeePayrollSalary(k, v);
				employeeAdditionStatus.put(k.hashCode(), true);
				LOG.info("Employee updated : " + Thread.currentThread().getName());
				//System.out.println("Employee updated : " + Thread.currentThread().getName());
			};
			Thread thread = new Thread(task, k);
			thread.start();
		});
		while (employeeAdditionStatus.containsValue(false)) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * checks if add data updated is in sync
	 * 
	 * @param nameList
	 * @return
	 */
	public boolean checkEmployeeListSync(List<String> nameList) {
		List<Boolean> resultList = new ArrayList<>();
		nameList.forEach(name -> {
			resultList.add(checkEmployeePayrollInSyncWithDB(name));
		});
		if (resultList.contains(false)) {
			return false;
		}
		return true;
	}
	
	/**
	 * REST UC 1
	 * 
	 * Adding employee object to payroll added to json
	 * 
	 * @param employee
	 */
	public void addEmployeeToPayroll(EmployeePayrollData employee) {
		addEmployeeToPayroll(employee.name, employee.gender, employee.salary, employee.startDate, Arrays.asList(""));
	}

	public int countEntries(IOService io) {
		int count = 0;
		if (io.equals(IOService.REST_IO)) {
			count = employeePayrollList.size();
		}
		return count;
	}
	
	/**
	 * REST UC 5
	 * 
	 * deleting data from JSON server
	 * 
	 * @param name
	 */
	public void deleteEmployeeJSON(String name) {
		EmployeePayrollData employee = this.getEmployeePayrollData(name);
		employeePayrollList.remove(employee);
	}
}