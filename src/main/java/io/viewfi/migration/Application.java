package io.viewfi.migration;


import java.text.ParseException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Application {

    public static void main(String[] args) {

        try {

            clickHouseMigration();

            mongoMigration();

        } catch (Exception e) {
            System.out.println("main  ------");
            e.printStackTrace();
        }

    }

    private static void clickHouseMigration() throws ParseException, ExecutionException, InterruptedException {

        DBInsertion dbInsertion = new DBInsertion();
        CHQuery chQuery = new CHQuery();

        List<EventsReport> eventsReportList = chQuery.getEventsReport();
        dbInsertion.insertEventsReport(eventsReportList);

        List<Claim> claimList = chQuery.getClaims();
        dbInsertion.insertClaims(claimList);

    }

    private static void mongoMigration(){
        DBInsertion dbInsertion = new DBInsertion();
        MongoQuery mongoQuery = new MongoQuery();

        List<Viewer> viewerList =  mongoQuery.getViewers();
        dbInsertion.insertViewers(viewerList);

    }

}
