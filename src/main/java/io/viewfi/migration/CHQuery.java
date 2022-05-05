package io.viewfi.migration;

import com.clickhouse.client.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CHQuery {

    ClickHouseNode server;

    public CHQuery() {

        server = ClickHouseNode.builder()
                .host(System.getProperty("chHost", "localhost"))
                .port(ClickHouseProtocol.HTTP, Integer.parseInt(System.getProperty("chPort", "8123")))
                .database("system").credentials(ClickHouseCredentials.fromUserAndPassword(
                        System.getProperty("chUser", "default"), System.getProperty("chPassword", "")))
                .build();
    }

    public List<EventsReport> getEventsReport() throws ExecutionException, InterruptedException, ParseException {

        List<EventsReport> result = new ArrayList<>();
        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        var query = """ 
                        SELECT ORGANIZATION_ID,
                                ACCOUNT_ID,
                                CAMPAIGN_ID,
                                toDate(DATETIME) as DATE,
                                EVENT_NAME,
                                sum(EVENT_COUNT) as COUNT
                        FROM reporter.EVENTSREPORT
                        GROUP BY toDate(DATETIME),
                                EVENT_NAME,
                                CAMPAIGN_ID,
                                ACCOUNT_ID,
                                ORGANIZATION_ID
                """;

        ClickHouseFormat preferredFormat = ClickHouseFormat.RowBinaryWithNamesAndTypes;

        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
             ClickHouseResponse response = client.connect(server)
                     .format(preferredFormat)
                     .query(query)
//                     .params(10)
                     .execute().get()) {

            for (ClickHouseRecord record : response.records()) {

                if (record.iterator().hasNext()) {

                    ClickHouseValue clickHouseValue = record.iterator().next();
                    String values[] = clickHouseValue.asString().split("\t");

                    EventsReport item = new EventsReport();
                    item.organizationId = values[0];
                    item.accountId = values[1];
                    item.campaignId = values[2];
                    item.date = simpleDateFormat.parse(values[3]);
                    item.eventName = values[4];
                    item.count = Integer.parseInt(values[5]);
                    result.add(item);

//                    System.out.print("ORGANIZATION_ID: " + values[0]);
//                    System.out.print(", ACCOUNT_ID: " + values[1]);
//                    System.out.print(", CAMPAIGN_ID: " + values[2]);
//                    System.out.print(", DATE: " + values[3]);
//                    System.out.print(", EVENT_NAME: " + values[4]);
//                    System.out.print(", COUNT: " + values[5]);
//                    System.out.println("  \n");

                }
            }
        }

        return result;
    }

    public List<Claim> getClaims() throws ExecutionException, InterruptedException, ParseException {

        List<Claim> result = new ArrayList<>();
        String pattern = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        var query = """ 
                SELECT ORGANIZATION_ID,
                       ACCOUNT_ID,
                       CAMPAIGN_ID,
                       VIEWER_ID,
                       DATETIME,
                       FIRSTNAME,
                       LASTNAME,
                       EMAIL,
                       MOBILENUMBER,
                       REWARD,
                       REFERRER,
                       COUNTRY,
                       IP,
                       DATA1,
                       DATA2,
                       DATA3,
                       CAMPAIGN_SOURCE,
                       CAMPAIGN_NAME,
                       CRM_CAMPAIGN_NAME,
                       WALLET_ACCOUNT
                FROM reporter.CLAIMS
                """;

        ClickHouseFormat preferredFormat = ClickHouseFormat.RowBinaryWithNamesAndTypes;

        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
             ClickHouseResponse response = client.connect(server)
                     .format(preferredFormat)
                     .query(query)
//                     .params(10)
                     .execute().get()) {

            for (ClickHouseRecord record : response.records()) {

                if (record.iterator().hasNext()) {
                    ClickHouseValue clickHouseValue = record.iterator().next();
                    String values[] = clickHouseValue.asString().split("\t", -1);

                    Claim item = new Claim();
                    item.organizationId = values[0];
                    item.accountId = values[1];
                    item.campaignId = values[2];
                    item.viewerId = values[3];
                    item.dateTime = simpleDateFormat.parse(values[4]);
                    item.firstName = values[5];
                    item.lastName = values[6];
                    item.email = values[7];
                    item.mobileNumber = values[8];
                    item.reward = values[9];
                    item.referrer = values[10];
                    item.country = values[11];
                    item.ip = values[12];
                    item.optionalData1 = values[13];
                    item.optionalData2 = values[14];
                    item.optionalData3 = values[15];
                    item.campaignSource = values[16];
                    item.campaignName = values[17];
                    item.crmCampaignName = values[18];
                    item.walletAccount = values[19];

                    if (!(item.viewerId.trim().equals("17766021F2300010AF40131")
                            && (item.campaignId.trim().equals("178F6BB606")))) {
                        result.add(item);
                    }

//                    System.out.print("ORGANIZATION_ID: " + values[0]);
//                    System.out.print(", ACCOUNT_ID: " + values[1]);
//                    System.out.print(", CAMPAIGN_ID: " + values[2]);
//                    System.out.print(", VIEWER_ID: " + values[3]);
//                    System.out.print(", DATETIME : " + values[4]);
//                    System.out.print(", FIRSTNAME: " + values[5]);
//                    System.out.print(", LASTNAME: " + values[6]);
//                    System.out.print(", EMAIL: " + values[7]);
//                    System.out.print(", MOBILENUMBER: " + values[8]);
//                    System.out.print(", REWARD: " + values[9]);
//                    System.out.print(", REFERRER: " + values[10]);
//                    System.out.print(", COUNTRY: " + values[11]);
//                    System.out.print(", IP: " + values[12]);
//                    System.out.print(", DATA1: " + values[13]);
//                    System.out.print(", DATA2: " + values[14]);
//                    System.out.print(", DATA3: " + values[15]);
//                    System.out.print(", CAMPAIGN_SOURCE: " + values[16]);
//                    System.out.print(", CAMPAIGN_NAME: " + values[17]);
//                    System.out.print(", CRM_CAMPAIGN_NAME: " + values[18]);
//                    System.out.println("  \n");
                }
            }
        }

        return result;
    }

}
