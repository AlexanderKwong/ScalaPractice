package com.qtone.entity;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kwong on 2017/3/30.
 */
public class ExamPaperSubjectQuestionRule implements Serializable{

    private String questionId;

    private String standardAnswer;

    private Map<String, Float> answerScoreMapping = new HashMap<>();

    public ExamPaperSubjectQuestionRule(String questionId, String standardAnswer, String rule){
        this.questionId = questionId;
        this.standardAnswer = standardAnswer;
        init(rule);
    }

    // [{"values":[{"name":"A","score":4},{"name":"A,C","score":8},{"name":"C","score":4},{"name":"D","score":0}],"type":"group"},{},{},{}]
    private void init(String rules){
        // 解析json串
        JSONArray array = JSONObject.parseArray(rules);

//        String result = getSortAnswer(stuAnswer);

        for (int i = 0; i < array.size(); i++) {
            JSONObject jsonObject = array.getJSONObject(i);
            String type = (String) jsonObject.get("type");
            JSONArray values = jsonObject.getJSONArray("values");
            if (type.equalsIgnoreCase("group")) {
                for (int j = 0; j < values.size(); j++) {
                    JSONObject value = values.getJSONObject(j);
                    String name = value.getString("name");
                    answerScoreMapping.put(getSortAnswer(name), value.getFloat("score"));
                }
            }
        }
    }

    private String getSortAnswer(String answer) {

        if (answer.contains(",")) {

            String[] an = answer.toLowerCase().split(",");
            Arrays.sort(an);
            StringBuilder builder = new StringBuilder();
            for (String s : an) {
                builder.append(s);
            }
            return builder.toString();
        }

        return answer;

    }

    public Float scoreValueFromAnswer(String answer){
        if (StringUtils.isEmpty(answer)) return 0f;
        Float score = answerScoreMapping.get(getSortAnswer(answer));
        return score == null? 0f:score;
    }

  /*  public static void main(String[] args){
        init("[{\"values\":[{\"name\":\"A\",\"score\":1,\"show\":1,\"custom\":0},{\"name\":\"A,B\",\"score\":1,\"show\":1,\"custom\":0},{\"name\":\"A,B,C\",\"score\":\"3\",\"show\":1,\"custom\":0},{\"name\":\"A,C\",\"score\":2,\"show\":1,\"custom\":0},{\"name\":\"B\",\"score\":1,\"show\":1,\"custom\":0},{\"name\":\"B,C\",\"score\":1,\"show\":1,\"custom\":0},{\"name\":\"C\",\"score\":1,\"show\":1,\"custom\":0}],\"type\":\"group\"}]");
        System.out.println(scoreValueFromAnswer("assas"));
    }*/
}
