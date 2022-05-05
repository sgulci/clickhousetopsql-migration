package io.viewfi.migration;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;

public class Viewer {
    public String email;
    public String firstName;
    public String lastName;
    public String mobileNumber;
    public String viewerKey;
    public Date created;
    public List<Perk> perkList = new ArrayList<>();
}
