package de.l3s.warcpartitioner.utils;

import de.l3s.warcpartitioner.WarcPartitioner;
import org.joda.time.LocalDateTime;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

public final class WarcHeaders {
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private WarcHeaders() {}

    public static byte[] getArcHeader(String filename) {
        StringBuilder header = new StringBuilder();
        header.append("filedesc://");
        header.append(filename);
        header.append(" 0.0.0.0 "); // ip
        LocalDateTime now = new LocalDateTime();
        header.append("" + now.getYear() + td(now.getMonthOfYear()) + td(now.getDayOfMonth()) + td(now.getHourOfDay()) + td(now.getMinuteOfHour()) + td(now.getSecondOfMinute()));
        header.append(" text/plain ");

        StringBuilder headerBody = new StringBuilder();
        headerBody.append("1 0 InternetArchive\r\n"); // Internet Archive: Name of gathering organization with no white space (http://archive.org/web/researcher/ArcFileFormat.php)
        headerBody.append("URL IP-address Archive-date Content-type Archive-length");

        String headerBodyStr = headerBody.toString();
        byte[] headerBodyBlob = headerBodyStr.getBytes(UTF8);

        header.append(headerBodyBlob.length);
        header.append("\r\n");
        header.append(headerBodyStr);
        header.append("\r\n\r\n");

        return header.toString().getBytes(UTF8);
    }

    public static byte[] getWarcHeader(String filename) {
        StringBuilder header = new StringBuilder();
        header.append("WARC/1.0\r\n");
        header.append("WARC-Type: warcinfo\r\n");
        LocalDateTime now = new LocalDateTime();
        String timestamp = now.getYear() + "-" + td(now.getMonthOfYear()) + "-" + td(now.getDayOfMonth()) + "T" + td(now.getHourOfDay()) + ":" + td(now.getMinuteOfHour()) + ":" + td(now.getSecondOfMinute()) + ".000Z";
        header.append("WARC-Date: " + timestamp + "\r\n");
        header.append("WARC-Filename: " + filename + "\r\n");
        header.append("WARC-Record-ID: <" + warcRecordID() + ">\r\n");
        header.append("Content-Type: application/warc-fields\r\n");

        StringBuilder headerBody = new StringBuilder();
        headerBody.append("software: " + WarcPartitioner.class.getCanonicalName() + "\r\n");
        headerBody.append("format: WARC File Format 1.0\r\n");
        headerBody.append("conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf\r\n");
        headerBody.append("publisher: Internet Archive\r\n");
        headerBody.append("created: " + timestamp + "\r\n");
        headerBody.append("\r\n\r\n\r\n");

        String headerBodyStr = headerBody.toString();
        byte[] headerBodyBlob = headerBodyStr.getBytes(UTF8);

        header.append("Content-Length: " + headerBodyBlob.length + "\r\n");
        header.append("\r\n");
        header.append(headerBodyStr);

        return header.toString().getBytes(UTF8);
    }

    private static String td(int i) { return (i < 10 ? "0" + i : "" + i); } // two digits

    private static String warcRecordID() { return "urn:uuid:" + UUID.randomUUID().toString(); }

    public static void main(String [] args) throws IOException {
        System.out.print(new String(WarcHeaders.getArcHeader("test.arc.gz"), WarcHeaders.UTF8));
        System.out.print(new String(WarcHeaders.getWarcHeader("test.warc.gz"), WarcHeaders.UTF8));
        System.out.print("end");
    }
}
