package it.polito.tdp.extflightdelays.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import it.polito.tdp.extflightdelays.db.ExtFlightDelaysDAO;

public class Model {
	
						  //tipologia di grafo
	private Graph<Airport, DefaultWeightedEdge> grafo;
	private ExtFlightDelaysDAO dao;
	
	//conviene creare una mappa per non fare sempre query -> di solito si fa cosi con i grafi
	private Map<Integer, Airport> idMap; //associa l'id all'aereoporto
	
	public Model() {
		this.dao = new ExtFlightDelaysDAO();
		this.idMap = new HashMap<Integer, Airport>();
		this.dao.loadAllAirports(idMap);
	}
	
	
	/**
	 * metodo che restituisce il numero di vertici nel grafo
	 * @return
	 */
	public int nVertici() {
		return this.grafo.vertexSet().size();
	}
	
	
	/**
	 * metodo che restituisce il numero di archi nel grafo
	 * @return
	 */
	public int nArchi() {
		return this.grafo.edgeSet().size();
	}
	

	
	/**
	 * Metodo che drea il grafo dato in ingresso un parametro per filtrare
	 * i vertici (cioè gli aeroporti) 
	 * @param nAirlines
	 */                   //nAirlines sarà l'input utente
	public void creaGrafo(int nAirlines) {
		// creazione grafo qui, così il grafo si resetta se lo ricostruiamo con
		// un nuovo valore per il parametro di ingresso
		this.grafo = new SimpleWeightedGraph(DefaultWeightedEdge.class);
		
		// aggiunta vertici
		Graphs.addAllVertices(grafo, this.dao.getVertici(nAirlines, idMap));
		
		// aggiunta archi
		// prima leggiamo dal dao tutte le rotte, senza preoccuparci del verso
		// di percorrenza
		List<Rotta> edges = this.dao.getRotte(idMap);
	
		/* Poi cicliamo su tutte le rotte
		 * se i vertici della rotta sono presenti nel grafo (ovvero rientrano tra gli aeroporti
		 * filtrati, allora possiamo creare l'arco, con il peso della rotta.
		 * Se l'arco già esiste, allora significa che ora stiamo guardando la rotta opposta,
		 * quindi basta aggiornare il peso dell'arco aggiungendoci il numero di voli della rotta opposta
		 */
		for (Rotta e : edges) {
			Airport origin = e.getOrigin();
			Airport destination = e.getDestination();
			int nVoli = e.getNVoli();
			
			//dobbiamo controllare se i vertici dell'arco sono nel grafo. In 
			// caso negativo, non possiamo aggiungerlo!
			if(grafo.containsVertex(origin) && 
					grafo.containsVertex(destination)) {
				DefaultWeightedEdge edge = this.grafo.getEdge(origin, destination);
				if (edge !=null) {
					double weight = this.grafo.getEdgeWeight(edge);
					weight += nVoli;
					this.grafo.setEdgeWeight(origin, destination, weight);
				} else {
					this.grafo.addEdge(origin, destination);
					this.grafo.setEdgeWeight(origin, destination, nVoli);
					//L'aggiunta la si può anche fare il metodo della libreria Gaphs
					// Graphs.addEdgeWithVertices(this.grafo, origin, destination, nVoli);
				}
			}
			
			
		}
		System.out.println("Grafo creato");
		System.out.println("ci sono " + this.grafo.vertexSet().size() + " vertici");
		System.out.println("ci sono " + this.grafo.edgeSet().size() + " edges");
	}
	
	
	
	
	/**
	 * Versione alternativa per creare il grafo, sfruttando una query che aggrega già
	 * il numero di voli di rotte opposte. Si semplifica un pochino il codice qui, ma
	 * si complica di molto la query
	 * @param nAirlines
	 */
	public void creaGrafo2(int nAirlines) {
		// creazione grafo qui, così il grafo si resetta se lo ricostruiamo con
		// un nuovo valore per il parametro di ingresso
		this.grafo = new SimpleWeightedGraph(DefaultWeightedEdge.class);
		
		// aggiunta vertici
		Graphs.addAllVertices(grafo, this.dao.getVertici(nAirlines, idMap));
		
		// aggiunta archi
		// prima leggiamo dal dao tutte le rotte, senza preoccuparci del verso
		// di percorrenza
		List<Rotta> edges = this.dao.getRotteAggregate(idMap);
	
		/* Poi cicliamo su tutte le rotte
		 * se i vertici della rotta sono presenti nel grafo (ovvero rientrano tra gli aeroporti
		 * filtrati, allora possiamo creare l'arco, con il peso della rotta.
		 * Visto che le rotte sono già aggregate, non serve controllare che un arco sia già presente
		 */
		for (Rotta e : edges) {
			Airport origin = e.getOrigin();
			Airport destination = e.getDestination();
			int nVoli = e.getNVoli();
			
			//dobbiamo controllare se i vertici dell'arco sono nel grafo. In 
			// caso negativo, non possiamo aggiungerlo!
			if(grafo.containsVertex(origin) && 
					grafo.containsVertex(destination)) {
				DefaultWeightedEdge edge = this.grafo.getEdge(origin, destination);
					this.grafo.addEdge(origin, destination);
					this.grafo.setEdgeWeight(origin, destination, nVoli);
					//L'aggiunta la si può anche fare il metodo della libreria Gaphs
					// Graphs.addEdgeWithVertices(this.grafo, origin, destination, nVoli);
			}
			
			
		}
		System.out.println("Grafo creato");
		System.out.println("ci sono " + this.grafo.vertexSet().size() + " vertici");
		System.out.println("ci sono " + this.grafo.edgeSet().size() + " edges");
	}
	
	
	
	/**
	 * Metodo getter che restituisce i vertici del grafo. Serve per popolare le tendine 
	 * nell'interfaccia grafica, una volta che il grafo é stato creato
	 * @return
	 */
	public List<Airport> getVertici(){
		List<Airport> vertici = new ArrayList<>(this.grafo.vertexSet());
		Collections.sort(vertici);
		return vertici;
	}
	
	
	
	/**
	 * Metodo per verificare se due aeroporti sono connessi nel grafo, e quindi se esiste un percorso tra i due
	 * @param origin
	 * @param destination
	 * @return
	 */
	public boolean esistePercorso(Airport origin, Airport destination) {
		ConnectivityInspector<Airport, DefaultWeightedEdge> inspect = new ConnectivityInspector<Airport, DefaultWeightedEdge>(this.grafo);
		Set<Airport> componenteConnessaOrigine = inspect.connectedSetOf(origin);
		return componenteConnessaOrigine.contains(destination);
	}
	
	
	
	/**
	 * Metodo che calcola il percorso tra due aeroporti. Se il percorso non viene trovato, 
	 * il metodo restituisce null. VEDI METRO DAO
	 * @param origin
	 * @param destination
	 * @return
	 */
	public List<Airport> trovaPercorso(Airport origin, Airport destination){
		List<Airport> percorso = new ArrayList<>();
	 	BreadthFirstIterator<Airport,DefaultWeightedEdge> it = new BreadthFirstIterator<>(this.grafo, origin);
	 	Boolean trovato = false;
	 	
	 	//visito il grafo fino alla fine o fino a che non trovo la destinazione
	 	while(it.hasNext() & !trovato) {
	 		Airport visitato = it.next();
	 		if(visitato.equals(destination))
	 			trovato = true;
	 	}
	 
	 
	 	/* se ho trovato la destinazione, costruisco il percorso risalendo l'albero di visita in senso
	 	 * opposto, ovvero partendo dalla destinazione fino ad arrivare all'origine, ed aggiiungo gli aeroporti
	 	 * ad ogni step IN TESTA alla lista
	 	 * se non ho trovato la destinazione, restituisco null.
	 	 */
	 	if(trovato) {
	 		percorso.add(destination);
	 		Airport step = it.getParent(destination);
	 		while (!step.equals(origin)) {
	 			percorso.add(0,step);
	 			step = it.getParent(step);
	 		}
		 
		 percorso.add(0,origin);
		 return percorso;
	 	} else {
	 		return null;
	 	}
		
	}
	
	
	public List<Airport> getAllAirports(){
		return this.dao.loadAllAirports();
	}
	
	
	
}


















