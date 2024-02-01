package cis5550.tools;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

//TODO: Accurate test for processHtmlString
public class HtmlProcessor {

    public static List<String> processHtmlString(String html) {
        String noHtml = html.replaceAll("<[^>]*>", "");
        String noPunctuation = noHtml.replaceAll("\\p{Punct}", "");
        String noWhitespace = noPunctuation.replaceAll("\\s+", " ");
        String lowerCase = noWhitespace.toLowerCase();
        return Arrays.stream(lowerCase.split("\\s+"))
                .filter(word -> !word.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        String htmlString = Box.str;
        var processedString = processHtmlString(htmlString);
        System.out.println(processedString);
    }

    static class Box {
        public static String str = "";
    }
}
