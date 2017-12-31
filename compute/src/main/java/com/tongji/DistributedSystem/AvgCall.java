package com.tongji.DistributedSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AvgCall {
    public static void main(String[] args) {
        //<主叫号码,有通话记录的天数>
        HashMap<String, Integer> dayCount = new HashMap<>();
        //<主叫号码,所有通话次数>
        HashMap<String, Integer> allCount = new HashMap<>();
        //<主叫号码,通话日期>
        HashMap<String, ArrayList<String>> teleDate = new HashMap<>();
        //<主叫号码,每日平均通话次数>
        HashMap<String, Float> avgCount = new HashMap<>();
        String FilePath = "dataset/tb_call_201202_random.txt";
        try {
            StringBuffer sb= new StringBuffer("");
            FileReader reader = new FileReader(FilePath);
            BufferedReader br = new BufferedReader(reader);
            String str = null;
            while((str = br.readLine()) != null) {
                str += "\r\n";
                String[] dictionary = str.split("\\s{2,}|\t");
                sb.append(dictionary[0] + "," + dictionary[1] + "\n");
                ArrayList<String> tempList = new ArrayList<>();
                if(teleDate.containsKey(dictionary[1])){
                    ArrayList<String> preList = teleDate.get(dictionary[1]);
                    boolean needUpdate = true;
                    for(String dateString : preList){
                        // 该日期之前已存在
                        if(dateString.equals(dictionary[0])){
                            needUpdate = false;
                            break;
                        }
                    }
                    // 不存在,需要添加
                    if(needUpdate) {
                        preList.add(dictionary[0]);
                        teleDate.put(dictionary[1], preList);
                    }
                }
                else{
                    tempList.add(dictionary[0]);
                    teleDate.put(dictionary[1], tempList);
                }
                if(allCount.containsKey(dictionary[1])) {
                    // 重复key值value加一
                    int temp = allCount.get(dictionary[1])+1;
                    allCount.replace(dictionary[1], temp);
                }
                else{
                    allCount.put(dictionary[1], 1);
                }
            }
            br.close();
            reader.close();
            for(Map.Entry<String, ArrayList<String>> entry : teleDate.entrySet()) {
                dayCount.put(entry.getKey(),entry.getValue().size());
            }
            // 计算日平均通话次数
            for(Map.Entry<String, Integer> entry : allCount.entrySet()) {
                if(dayCount.containsKey(entry.getKey())){
                    avgCount.put(entry.getKey(), (((float)entry.getValue()/dayCount.get(entry.getKey()))));
                }
            }
            // 结果写入Txt文件
            File writename = new File("avg_call.txt");
            writename.createNewFile();
            BufferedWriter out = new BufferedWriter(new FileWriter(writename));
            out.write("TeleNumber" + " : " + "Count" + "\r\n");
            for(Map.Entry<String, Float> entry : avgCount.entrySet()) {
                out.write(entry.getKey() + " : " + entry.getValue() + "\r\n"); // \r\n即为换行
            }
            out.flush();
            out.close();
        }
        catch(FileNotFoundException e) {
            e.printStackTrace();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }
}
