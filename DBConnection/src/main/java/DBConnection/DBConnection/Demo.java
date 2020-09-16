package DBConnection.DBConnection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Demo {
	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final String SCHEME = "http";
	private static RestHighLevelClient restHighLevelClient;
	private static final String INDEX = "persondata";
	private static final String TYPE = "person";
	private static ObjectMapper objectMapper = new ObjectMapper();
// GET REQUEST: GET persondata/_doc/f596b252-bb7b-4e43-ae6d-6efb8830f2fb here f596b252-bb7b-4e43-ae6d-6efb8830f2fb is the UUId random no

	public static void main(String[] args) throws IOException {

		 makeConnection();
		 
		    System.out.println("Inserting a new Person with name TestUser");
		    Person person = new Person();
		    person.setName("TestUser");
		    person = insertPerson(person);
		    System.out.println("Person inserted --> " + person);
		    
		    System.out.println("Changing name to TestUser1...");
		    person.setName("TestUser1");
		    updatePersonById(person.getPersonId(), person);
		    System.out.println("Person updated  --> " + person);
		 
		    
		    System.out.println("Getting TestUser...");
		    Person personFromDB = getPersonById(person.getPersonId());
		    System.out.println("Person from DB  --> " + personFromDB);
		    closeConnection();

	}

	private static synchronized RestHighLevelClient makeConnection() {

		if(restHighLevelClient == null) {
			restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(
							new HttpHost(HOST, PORT_ONE, SCHEME)));



		}
		return restHighLevelClient;

	}
	
	
	private static Person insertPerson(Person person){
	    person.setPersonId(UUID.randomUUID().toString());
	    Map<String, Object> dataMap = new HashMap<String, Object>();
	    dataMap.put("personId", person.getPersonId());
	    dataMap.put("name", person.getName());
	    IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, person.getPersonId())
	            .source(dataMap);
	    try {
	        IndexResponse response = restHighLevelClient.index(indexRequest);
	    } catch(ElasticsearchException e) {
	        e.getDetailedMessage();
	    } catch (java.io.IOException ex){
	        ex.getLocalizedMessage();
	    }
	    return person;
	}
	
	private static Person getPersonById(String id){
	    GetRequest getPersonRequest = new GetRequest(INDEX, TYPE, id);
	    GetResponse getResponse = null;
	    try {
	        getResponse = restHighLevelClient.get(getPersonRequest);
	    } catch (java.io.IOException e){
	        e.getLocalizedMessage();
	    }
	    return getResponse != null ?
	            objectMapper.convertValue(getResponse.getSourceAsMap(), Person.class) : null;
	}
	
	private static Person updatePersonById(String id, Person person){
	    UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id)
	            .fetchSource(true);    // Fetch Object after its update
	    try {
	        String personJson = objectMapper.writeValueAsString(person);
	        updateRequest.doc(personJson, XContentType.JSON);
	        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
	        return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Person.class);
	    }catch (JsonProcessingException e){
	        e.getMessage();
	    } catch (java.io.IOException e){
	        e.getLocalizedMessage();
	    }
	    System.out.println("Unable to update person");
	    return null;
	}
	
	private static synchronized void closeConnection() throws IOException {
	    restHighLevelClient.close();
	    restHighLevelClient = null;
	}


	
	

}
