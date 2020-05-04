package example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// Handler value: example.Handler
public class Handler implements RequestHandler<Map<String,Object>, APIGatewayProxyResponseEvent>{
  private static final String USE_CACHE = "USE_CACHE";
  private static final String REQUEST_TYPE = "REQUEST";
  private static final String SQLS = "SQLS";


  Gson gson = new GsonBuilder().setPrettyPrinting().create();
  Cache cacheClient = new Cache();
  DbClient dbClient;

  @Override
  public APIGatewayProxyResponseEvent handleRequest(Map<String,Object> event, Context context)
  {
    if (event.containsKey("body")) {
      event = gson.fromJson((String) event.get("body"), Map.class);
    }
    try {
      boolean useCache = ((String) event.getOrDefault(USE_CACHE, "False")).equalsIgnoreCase("true");

      if (event.get(REQUEST_TYPE).equals("write")) {
        List<Map<String, Object>> records = (List<Map<String, Object>>) event.get(SQLS);

        List<HeroRecord> heroRecords = HeroRecord.convert(records);
        dbClient = new DbClient();
        heroRecords = dbClient.insert(heroRecords);

        if (useCache) {
          for (HeroRecord r : heroRecords) {
            context.getLogger().log("key is " + r.id + "\n");
            cacheClient.write(r.id.toString(), r);
          }
        }

        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", 200);
        response.put("body", "write success");

        return returnResponse(response);

      } else if (event.get(REQUEST_TYPE).equals("read")) {


        List<Object> sqlsParam = (List<Object>) event.get(SQLS);
        List<String> ids = sqlsParam.stream().map(o -> {
          if (o instanceof Double) {
            double oDouble = (Double) o;
            Integer oInt = (int) oDouble;
            return oInt.toString();
          } else {
            return o.toString();
          }
        }).collect(Collectors.toList());

        List<HeroRecord> results = new ArrayList<>();
        List<String> cacheMiss = new ArrayList<>();

        if (useCache) {
          for (String id : ids) {
            context.getLogger().log("read key is " + id + "\n");
            Object o = cacheClient.read(id.toString());
            if (o != null) {
              results.add((HeroRecord) o);
            } else {
              cacheMiss.add(id);
            }
          }
        } else {
          cacheMiss = ids;
        }


        dbClient = new DbClient();
        List<HeroRecord> fetchedFromDb = dbClient.fetch(cacheMiss);
        results.addAll(fetchedFromDb);

        if (useCache) {
          context.getLogger().log("cache miss is " + cacheMiss + "\n");
          for (HeroRecord r : fetchedFromDb) {
            cacheClient.write(r.id.toString(), r);
          }
        }

        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", 200);
        response.put("body", results);

        return returnResponse(response);

      } else {
        throw new UnsupportedOperationException("Unsupported request type!");
      }
    } catch(Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      if (dbClient != null) {
        dbClient.close();
      }
    }

  }

  private APIGatewayProxyResponseEvent returnResponse(Object body) {
    APIGatewayProxyResponseEvent apiGatewayProxyResponseEvent = new APIGatewayProxyResponseEvent();
    apiGatewayProxyResponseEvent.setStatusCode(200);
    apiGatewayProxyResponseEvent.setBody(gson.toJson(body));
    return apiGatewayProxyResponseEvent;
  }

  private void log(Map<String,Object> event, Context context) {
    LambdaLogger logger = context.getLogger();
//     log execution details
    logger.log("ENVIRONMENT VARIABLES: " + gson.toJson(System.getenv()));
    logger.log("CONTEXT: " + gson.toJson(context));
//     process event
    logger.log("EVENT: " + gson.toJson(event));
    logger.log("EVENT TYPE: " + event.getClass().toString());
  }

}