package brickhouse.udf.json;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Check if a given string is a valid JSON.
 */
@Description(name = "is_json",
    value = "_FUNC_(json) - Returns true if the string is a valid JSON, false otherwise."
)
public class IsJsonUDF extends UDF {
  private StringObjectInspector stringInspector;

  public Boolean evaluate(String jsonString) {
    if (jsonString == null)
      return false;

    ObjectMapper jacksonParser = new ObjectMapper();
    try {
      JsonNode jsonNode = jacksonParser.readTree(jsonString);
      return true;  // If parsing is successful, return true
    } catch (JsonProcessingException e) {
      return false; // If there's a parsing error, return false
    } catch (IOException e) {
      return false; // If there's an IO error, return false
    }
  }

  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("is_json expects exactly one argument: a string");
    }

    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE
        || ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UDFArgumentException("is_json expects a string");
    }

    stringInspector = (StringObjectInspector) arguments[0];

    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
  }
}
