package io.viewfi.migration;

import java.sql.*;
import java.util.List;

public class DBInsertion {
    private final String url = System.getProperty("pgHost", "jdbc:postgresql://localhost/viewfi");
    private final String user =  System.getProperty("pgUser", "postgres");
    private final String password = System.getProperty("pgPort", "12345");

    /**
     * Connect to the PostgreSQL database
     *
     * @return a Connection object
     */
    public Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public DBInsertion() {
    }

    public void insertViewers(List<Viewer> list) {

        String INSERT_VIEWER_SQL = """
                                    INSERT INTO widget.viewers
                                    (viewer_key, first_name, last_name, email,
                                     mobile_number, created)
                                    VALUES (?, ?, ?, ?,
                                            ?, ?)
                 
                """;

        try (
                Connection conn = connect();
                PreparedStatement statement = conn.prepareStatement(INSERT_VIEWER_SQL, Statement.RETURN_GENERATED_KEYS)) {

            for (Viewer item : list) {

                statement.setString(1, item.viewerKey);
                statement.setString(2, item.firstName.replaceAll("\u0000", ""));
                statement.setString(3, item.lastName.replaceAll("\u0000", ""));
                statement.setString(4, item.email.replaceAll("\u0000", ""));
                statement.setString(5, item.mobileNumber);
                statement.setObject(6, item.created, Types.TIMESTAMP);
                statement.execute();

                ResultSet rs = statement.getGeneratedKeys();
                long generatedKey ;
                if (rs.next()) {
                    generatedKey = rs.getLong(1);
                    insertPerks(item.perkList, generatedKey);
                }


            }
        } catch (SQLException ex) {
            System.out.println(" insertViewers ------  ");
            System.out.println(ex.getMessage());
        }
    }

    private void insertPerks(List<Perk> list, long viewerId) {


        String INSERT_PERK_SQL = """
                                    INSERT INTO widget.perks
                                    (organization_id, account_id, campaign_id, viewer_id,
                                    created, reward, data1, data2, data3, wallet_account)
                                    VALUES (?, ?, ?, ?,
                                            ?, ?, ?, ?, ?, ?
                                            )
                """;
        try (
                Connection conn = connect();
                PreparedStatement statement = conn.prepareStatement(INSERT_PERK_SQL, Statement.RETURN_GENERATED_KEYS)) {
            int count = 0;

            for (Perk item : list) {
                statement.setString(1, item.organizationId);
                statement.setString(2, item.accountId);
                statement.setString(3, item.campaignId);
                statement.setLong(4, viewerId);
                statement.setObject(5, item.created, Types.TIMESTAMP);
                statement.setString(6, item.reward);
                statement.setString(7, item.optionalData1);
                statement.setString(8, item.optionalData2);
                statement.setString(9, item.optionalData3);
                statement.setString(10, item.walletAccount);

                statement.addBatch();
                count++;
                // execute every 100 rows or less
                if (count % 100 == 0 || count == list.size()) {
                    statement.executeBatch();
                }
            }
        } catch (SQLException ex) {
            System.out.println(" insertPerks ------  ");
            System.out.println(ex.getMessage());
        }
    }

    public void insertClaims(List<Claim> list) {

        String SQL = """
                                    INSERT INTO report.claims
                                    (organization_id, account_id, campaign_id, viewer_id,
                                    datetime, first_name, last_name, email, mobile_number,
                                    reward, data1, data2, data3, country, ip, referrer,
                                    campaign_name, campaign_source, crm_campaign_name, wallet_account)
                                    VALUES (?, ?, ?, ?,
                                            ?, ?, ?, ?, ?,
                                            ?, ?, ?, ?, ?, ?, ?,
                                            ?, ?, ?, ?
                                            )
                 
                """;
        try (
                Connection conn = connect();
                PreparedStatement statement = conn.prepareStatement(SQL)) {
            int count = 0;

            for (Claim item : list) {
                statement.setString(1, item.organizationId);
                statement.setString(2, item.accountId);
                statement.setString(3, item.campaignId);
                statement.setString(4, item.viewerId);
                statement.setObject(5, item.dateTime, Types.TIMESTAMP);
                statement.setString(6, item.firstName.replaceAll("\u0000", ""));
                statement.setString(7, item.lastName.replaceAll("\u0000", ""));
                statement.setString(8, item.email.replaceAll("\u0000", ""));
                statement.setString(9, item.mobileNumber.replaceAll("\u0000", ""));
                statement.setString(10, item.reward.replaceAll("\u0000", ""));
                statement.setString(11, item.optionalData1.replaceAll("\u0000", ""));
                statement.setString(12, item.optionalData2.replaceAll("\u0000", ""));
                statement.setString(13, item.optionalData3.replaceAll("\u0000", ""));
                statement.setString(14, item.country.replaceAll("\u0000", ""));
                statement.setString(15, item.ip.replaceAll("\u0000", ""));
                statement.setString(16, item.referrer.replaceAll("\u0000", ""));
                statement.setString(17, item.campaignName.replaceAll("\u0000", ""));
                statement.setString(18, item.campaignSource.replaceAll("\u0000", ""));
                statement.setString(19, item.crmCampaignName.replaceAll("\u0000", ""));
                statement.setString(20, item.walletAccount.replaceAll("\u0000", ""));

                statement.addBatch();
                count++;
                // execute every 100 rows or less
                if (count % 100 == 0 || count == list.size()) {
                    statement.executeBatch();
                }
            }
        } catch (SQLException ex) {
            System.out.println(" insertClaims ------  ");
            System.out.println(ex.getMessage());
        }
    }

    public void insertEventsReport(List<EventsReport> list) {

        String SQL = """
                                    INSERT INTO report.events_report
                                    (date, organization_id, account_id, campaign_id, event_name, event_count)
                                    VALUES (?, ?, ?, ?, ?, ?)
                 
                """;
        try (
                Connection conn = connect();
                PreparedStatement statement = conn.prepareStatement(SQL)) {
            int count = 0;

            for (EventsReport item : list) {
                statement.setObject(1, item.date, Types.DATE);
                statement.setString(2, item.organizationId);
                statement.setString(3, item.accountId);
                statement.setString(4, item.campaignId);
                statement.setString(5, item.eventName);
                statement.setInt(6, item.count);

                statement.addBatch();
                count++;
                // execute every 100 rows or less
                if (count % 100 == 0 || count == list.size()) {
                    statement.executeBatch();
                }
            }
        } catch (SQLException ex) {
            System.out.println(" insertEventsReport ------  ");
            System.out.println(ex.getMessage());
        }
    }
}
