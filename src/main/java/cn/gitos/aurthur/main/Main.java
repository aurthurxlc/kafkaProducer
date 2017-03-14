package cn.gitos.aurthur.main;

import cn.gitos.aurthur.avro.RcvblFlow;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/**
 * Created by Aurthur on 2017/3/11.
 */
public class Main {
    public final static String TOPIC = "TEST-TOPIC";

    public static void main(String[] args) {
        try {
            //消息生产
            produce();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void produce() throws IOException {
        Properties props = new Properties();
        props.put("metadata.broker.list", "e10:13390");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

        Random rnd = new Random();
        RcvblFlow msg = new RcvblFlow();
        for (int index = 0; index <= 100; index++) {
            msg.setRcvblAmtId(rnd.nextLong());
            msg.setAmtType("1");
            msg.setRcvblYm("201708");
            msg.setConsNo("1009");
            msg.setOrgNo("2009");
            msg.setPayMode("1");
            msg.setTPq(rnd.nextDouble());
            msg.setRcvblAmt(rnd.nextDouble());
            msg.setRcvedAmt(rnd.nextDouble());
            msg.setOwnAmt(rnd.nextDouble());
            msg.setReleasedDate("20170909");

            DatumWriter<RcvblFlow> msgDatumWriter = new SpecificDatumWriter<RcvblFlow>(RcvblFlow.class);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            msgDatumWriter.write(msg, encoder);
            encoder.flush();
            out.close();

            byte[] serializedBytes = out.toByteArray();

            KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(TOPIC, "rcvbl_flow", serializedBytes);
            producer.send(data);
        }
        producer.close();
    }

}
