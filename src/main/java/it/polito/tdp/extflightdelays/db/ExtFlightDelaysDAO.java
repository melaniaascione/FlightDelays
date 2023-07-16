package it.polito.tdp.extflightdelays.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import it.polito.tdp.extflightdelays.model.Airline;
import it.polito.tdp.extflightdelays.model.Airport;
import it.polito.tdp.extflightdelays.model.Flight;
import it.polito.tdp.extflightdelays.model.Rotta;


public class ExtFlightDelaysDAO {

	public List<Airline> loadAllAirlines() {
		String sql = "SELECT * from airlines";
		List<Airline> result = new ArrayList<Airline>();

		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				result.add(new Airline(rs.getInt("ID"), rs.getString("IATA_CODE"), rs.getString("AIRLINE")));
			}

			conn.close();
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}

	
	
	//carico la mappa che associa l'id all'aeroporto
	public void loadAllAirports(Map<Integer, Airport> idMap) {
		String sql = "SELECT * FROM airports";
		
		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				Airport airport = new Airport(rs.getInt("ID"), rs.getString("IATA_CODE"), rs.getString("AIRPORT"),
						rs.getString("CITY"), rs.getString("STATE"), rs.getString("COUNTRY"), rs.getDouble("LATITUDE"),
						rs.getDouble("LONGITUDE"), rs.getDouble("TIMEZONE_OFFSET"));
				idMap.put(rs.getInt("ID"), airport);
			}

			conn.close();
		

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}

	
	
	
	public List<Flight> loadAllFlights() {
		String sql = "SELECT * FROM flights";
		List<Flight> result = new LinkedList<Flight>();

		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				Flight flight = new Flight(rs.getInt("ID"), rs.getInt("AIRLINE_ID"), rs.getInt("FLIGHT_NUMBER"),
						rs.getString("TAIL_NUMBER"), rs.getInt("ORIGIN_AIRPORT_ID"),
						rs.getInt("DESTINATION_AIRPORT_ID"),
						rs.getTimestamp("SCHEDULED_DEPARTURE_DATE").toLocalDateTime(), rs.getDouble("DEPARTURE_DELAY"),
						rs.getDouble("ELAPSED_TIME"), rs.getInt("DISTANCE"),
						rs.getTimestamp("ARRIVAL_DATE").toLocalDateTime(), rs.getDouble("ARRIVAL_DELAY"));
				result.add(flight);
			}

			conn.close();
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}
	
	
	
	
	/**
	 * Metodo che legge dal dao tutti i vertici filtrati dal database, quindi il metodo mi
	 * da tutti gli aereoporti per cui lavorano più di nAirlines linee
	 * @param nAirlines
	 * @param idMap
	 * @return
	 */
	public List<Airport> getVertici(int nAirlines, Map<Integer, Airport>idMap){
		String sql = "SELECT tmp.ID, tmp.IATA_CODE, tmp.AIRLINE_ID, COUNT(*) as N "
				+ "FROM "
				+ "(SELECT a.ID, a.IATA_CODE, f.AIRLINE_ID, COUNT(*) as n "
				+ "FROM flights f, airports a "
				+ "WHERE f.ORIGIN_AIRPORT_ID = a.ID OR f.DESTINATION_AIRPORT_ID = a.ID "
				+ "GROUP BY a.ID, a.IATA_CODE, f.AIRLINE_ID)tmp "
				+ "GROUP BY tmp.ID, tmp.IATA_CODE "
				+ "HAVING N>=?";
		
		List<Airport> vertici = new ArrayList<Airport>();
		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1,  nAirlines);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				vertici.add(idMap.get(rs.getInt("ID")));
				
			}

			conn.close();
			return vertici;
			
			/* altro modo per scrivere questa query
			 *  SELECT a.ID, a.IATA_CODE
				FROM flights f, airports a
				WHERE f.ORIGIN_AIRPORT_ID = a.ID OR f.DESTINATION_AIRPORT_ID = a.ID
				GROUP BY a.ID, a.IATA_CODE
				HAVING COUNT(DISTINCT f.AIRLINE_ID) >10;
			 * 
			 */

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
		
	}
	
	
	
	/**
	 * Metodo che legge dal dao le rotte SENZA AGGREGARE LE ROTTE OPPOSTE!
	 * Quindi l'aggregazione va fatta poi nel codice quando si crea il grafo
	 * @param idMap
	 * @return
	 */
	public List<Rotta> getRotte(Map<Integer, Airport> idMap){
		String sql = "SELECT f.ORIGIN_AIRPORT_ID, f.DESTINATION_AIRPORT_ID, COUNT(*) as N "
				+ "	FROM flights f "
				+ "	GROUP BY f.ORIGIN_AIRPORT_ID, f.DESTINATION_AIRPORT_ID";
		
		
		List<Rotta> rotte = new ArrayList<Rotta>();
		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				rotte.add(new Rotta(idMap.get(rs.getInt("ORIGIN_AIRPORT_ID")),
						idMap.get(rs.getInt("DESTINATION_AIRPORT_ID")), rs.getInt("N")));	
			}

			conn.close();
			return rotte;

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}
	
	
	
	
	
	/**
	 * Metodo che legge dal dao le rotte AGGREGANDO LE ROTTE OPPOSTE! Quindi poi nel metodo 
	 * crea grafo non e' necessario sommare i pesi di rotte opposte. Però la query é molto più complicata....
	 * @param idMap
	 * @return
	 */
	public List<Rotta> getRotteAggregate(Map<Integer, Airport> idMap) {
		String sql = "SELECT T1.ORIGIN_AIRPORT_ID, T1.DESTINATION_AIRPORT_ID, COALESCE(T1.N, 0) + COALESCE(T2.N, 0) as N_VOLI "
				+ "FROM "
				+ "(SELECT f.ORIGIN_AIRPORT_ID, f.DESTINATION_AIRPORT_ID, COUNT(*) as N "
				+ "FROM flights f "
				+ "GROUP BY f.ORIGIN_AIRPORT_ID, f.DESTINATION_AIRPORT_ID) T1 "
				+ "LEFT JOIN "
				+ "(SELECT f.ORIGIN_AIRPORT_ID, f.DESTINATION_AIRPORT_ID, COUNT(*) as N "
				+ "FROM flights f "
				+ "GROUP BY f.ORIGIN_AIRPORT_ID, f.DESTINATION_AIRPORT_ID) T2 "
				+ "ON T1.ORIGIN_AIRPORT_ID = T2.DESTINATION_AIRPORT_ID AND T2.ORIGIN_AIRPORT_ID = T1.DESTINATION_AIRPORT_ID "
				+ "WHERE T1.ORIGIN_AIRPORT_ID < T2.ORIGIN_AIRPORT_ID OR T2.ORIGIN_AIRPORT_ID IS NULL OR T2.DESTINATION_AIRPORT_ID IS NULL";
		List<Rotta> rotte = new ArrayList<Rotta>();
		
		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				rotte.add(new Rotta(idMap.get(rs.getInt("ORIGIN_AIRPORT_ID")), 
						idMap.get(rs.getInt("DESTINATION_AIRPORT_ID")) , rs.getInt("N_VOLI"))  );
			}

			conn.close();
			return rotte;
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}
	
	
	
	//metodo che mi restituisce una LISTA di TUTTI GLI AEREOPORTI
		public List<Airport> loadAllAirports() {
			String sql = "SELECT * FROM airports";
			List<Airport> result = new ArrayList<Airport>();

			try {
				Connection conn = ConnectDB.getConnection();
				PreparedStatement st = conn.prepareStatement(sql);
				ResultSet rs = st.executeQuery();

				while (rs.next()) {
					Airport airport = new Airport(rs.getInt("ID"), rs.getString("IATA_CODE"), rs.getString("AIRPORT"),
							rs.getString("CITY"), rs.getString("STATE"), rs.getString("COUNTRY"), rs.getDouble("LATITUDE"),
							rs.getDouble("LONGITUDE"), rs.getDouble("TIMEZONE_OFFSET"));
					result.add(airport);
				}

				conn.close();
				return result;

			} catch (SQLException e) {
				e.printStackTrace();
				System.out.println("Errore connessione al database");
				throw new RuntimeException("Error Connection Database");
			}
		}
}