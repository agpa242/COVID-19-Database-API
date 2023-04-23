package cs505finaltemplate.CEP;

import cs505finaltemplate.Launcher;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import cs505finaltemplate.Topics.TestingData;
import io.siddhi.core.util.transport.InMemoryBroker;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.List;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");

            //You will need to parse output and do other logic,
            Launcher.lastCEPOutput = String.valueOf(msg);
            Map<String, Integer> tempList = new HashMap<>();
            String[] zipCodes = String.valueOf(msg).split("zip_code\":\"");
            int i = 0;
            String zip = "";
            for (String zipCode : zipCodes) {
                String[] sstr = zipCode.split("count\":");
                for (String unit : sstr) {
                    if (i != 0) {
                        if (i % 2 == 1) {
                            zip = unit.split("\"")[0];
                        }
                        else {
                            tempList.put(zip, Integer.parseInt(unit.split("}")[0]));
                        }
                    }
                    i += 1;
                }
            }

            Launcher.alerts = new ArrayList<>();
            for (Map.Entry<String, Integer> element : tempList.entrySet()) {
                Integer prevCount = Launcher.CEPList.get(element.getKey());
                if (prevCount != null) {
                    if (element.getValue() >= prevCount*2) {
                        Launcher.alerts.add(element.getKey());
                    }
                }
            }
            Launcher.CEPList = tempList;

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
