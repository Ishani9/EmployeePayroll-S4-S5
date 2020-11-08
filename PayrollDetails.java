package com.bl.jdbcassignment;

public class PayrollDetails {
	public double deductions;
	public double taxable_pay;
	public double tax;
	public double netPay;

	public PayrollDetails(double deductions, double taxable_pay, double tax, double netPay) {
		this.deductions = deductions;
		this.taxable_pay = taxable_pay;
		this.tax = tax;
		this.netPay = netPay;
	}

	public String toString() {
		return "Deductions = " + deductions + ", Taxable pay = " + taxable_pay + ", Tax = " + tax + ", Net Pay : "
				+ netPay;
	}
}