package it.uniroma3.grafiPriotita;

public class main {
	static Polystore polystore = new Polystore();
	public static void main (String[]args) throws Exception{
//		String query = "SELECT customer.first_name, customer.last_name, rental.rental_id FROM rental, customer, address, city WHERE city.city = 'Roma' AND address.city_id = city.city_id AND rental.customer_id = customer.customer_id AND address.address_id = customer.address_id";
		String query = "SELECT inventory.film_id, customer.address_id, address.address FROM rental, payment, customer, address, city, country, inventory WHERE inventory.inventory_id = rental.inventory_id AND rental.customer_id = customer.customer_id AND customer.address_id = address.address_id AND city.city_id = address.city_id AND rental.payment_id = payment.payment_id AND country.country_id = city.country_id";
//		String query = "db.rental.find({'rental.customer_id'=1})";
//		String query = "SELECT * FROM inventory";
//		String query = "SELECT customer.first_name, customer.last_name, payment.amount, address.address FROM rental, payment, customer, address WHERE rental.rental_id = payment.rental_id AND customer.customer_id = rental.customer_id AND customer.address_id = address.address_id";
		polystore.run(query);
	}
}
