package io.viewfi.migration;

import com.mongodb.client.*;
import org.bson.Document;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

public class MongoQuery {

    public MongoQuery() {
    }


    public List<Viewer> getViewers() {

        System.out.println("\n");
        List<Viewer> resultList = new ArrayList<>();
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern();
//
//        String pattern = "yyyy-MM-dd";
//        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");

        // Replace the uri string with your MongoDB deployment's connection string
        //  String uri = "mongodb://user:pass@sample.host:27017/?maxPoolSize=20&w=majority";
        String uri = System.getProperty("mURI", "mongodb://localhost:27017");
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("node");
            MongoCollection<Document> collection = database.getCollection("viewers");
            FindIterable<Document> docList = collection.find();

            for (Document doc : docList) {

                Viewer item = new Viewer();

                Document viewer = (Document) doc.get("object");
                item.viewerKey =  viewer.getString("id");
                item.email =  viewer.getString("email");
                item.firstName =  viewer.getString("firstName");
                item.lastName =  viewer.getString("lastName");
                item.mobileNumber =  viewer.getString("mobileNumber");
                item.created = new Date(doc.getLong("modified"));

                List<Document> perks = viewer.getList("perks", Document.class);

                if (perks != null) {
                    for (Document pDoc : perks) {

                        Perk perk = new Perk();
                        perk.organizationId = pDoc.getString("organizationId");
                        perk.accountId = pDoc.getString("accountId");
                        perk.campaignId = pDoc.getString("campaignId");
                        perk.optionalData1 = pDoc.getString("optionalData1");
                        perk.optionalData2 = pDoc.getString("optionalData2");
                        perk.optionalData3 = pDoc.getString("optionalData3");
                        perk.optionalData2 = pDoc.getString("optionalData2");
                        perk.reward = pDoc.getString("coupon");
                        perk.created = new Date();

                        if ((item.viewerKey.equals("1702D91625900010AF40121") || item.viewerKey.equals("17766021F2300010AF40131") ) &&
                                (perk.campaignId.equals("NGCPNTST") || perk.campaignId.equals("178F6BB606")))
                            continue;

                        item.perkList.add(perk);

                    }
                }

                resultList.add(item);
            }

        }

        return resultList;
    }
}
