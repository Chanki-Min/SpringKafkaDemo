package kr.ac.hongik.apl.demo.Sensor;

import org.springframework.stereotype.Component;

public class Sensor {
        public String date;
        public String time;

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public String getTime() {
            return time;
        }

        public void setTime(String time) {
            this.time = time;
        }

        public Sensor() {
        }

}
