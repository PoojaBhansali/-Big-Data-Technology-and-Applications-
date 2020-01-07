import java.io.IOException;
import java.io.PrintWriter;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Movie {
    public static void main(String args[]){

        Document document;

        try {
            System.out.println("running...");
            PrintWriter writer = new PrintWriter("movie.txt", "UTF-8");
            document = Jsoup.connect("https://www.boxofficemojo.com/daily/2018/?view=year&sort=date&sortDir=asc&ref_=bo_di__resort#table").get();

            Elements links = document.getElementsByClass("a-text-left mojo-field-type-release mojo-cell-wide");
            int day = 0;
            for (Element link : links) {
                String movie = link.text();
                day++;
                writer.println(movie + "#" + day);
            }
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("done!");
    }

}