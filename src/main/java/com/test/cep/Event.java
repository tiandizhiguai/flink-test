package com.test.cep;

public class Event {

	private int order;

	private String name;

	private double number;

	public Event(int order, String name, double number) {
		super();
		this.order = order;
		this.name = name;
		this.number = number;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getNumber() {
		return number;
	}

	public void setNumber(double number) {
		this.number = number;
	}

	@Override
	public String toString() {
		return "Event [order=" + order + ", name=" + name + ", number=" + number + "]";
	}

}
