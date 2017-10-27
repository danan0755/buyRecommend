package yks.com.java;

/**
 * Created by cgt on 17-10-24.
 */

import java.io.Serializable;
import java.util.regex.Pattern;

public class SkuDescFilterEn implements Serializable{
    private static final long serialVersionUID = 1L;
    //对商品的英文描述，过滤特殊字符和数字
    public static String evaluate(String text) {
        Pattern p = Pattern.compile("[a-zA-z]");
        String[] arr_word = text.split(" ");
        String word = "";
        for (int i = 0; i < arr_word.length; i++) {
            //剔除word'word格式的词
            if (!arr_word[i].matches("^\\w+'\\w+$")) {
                char[] StringArr = arr_word[i].toCharArray();
                String word_char = "";
                for (int j = 0; j < StringArr.length; j++) {
                    String s = String.valueOf(StringArr[j]);
                    //过滤字符和数字
                    String res = s.replaceAll("[\\pP|\\pS|\\pZ|\\pN]", "");
                    if (p.matcher(s).find() && res.length() > 0) {
                        word_char = word_char + res;
                    }
                }
                if (word_char.length() > 0) {
                    word = word + " " + word_char;
                }
            }
        }
        return word;
    }

}
