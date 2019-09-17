package com.onurtokat.stream;

import com.onurtokat.model.CashLoan;
import com.onurtokat.serde.JsonDeserializer;
import com.onurtokat.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.concurrent.CountDownLatch;

public class CashLoanFormatter {

    private static CashLoan cashLoan = new CashLoan();

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> cashLoanMainStream = builder.stream("cash-loan-topic");

/*
        cashLoanMainStream
                .map((KeyValueMapper<String, String, KeyValue<String, CashLoan>>) (key, value) -> {
                    String[] splitted = value.split(",");
                    cashLoan.setStatus(Integer.valueOf(splitted[1]));
                    cashLoan.setExchangecode(splitted[4]);
                    cashLoan.setLoanamount(Double.valueOf(splitted[5]));
                    cashLoan.setBranchcode(splitted[7]);
                    cashLoan.setValuedate(Long.valueOf(splitted[11]));
                    cashLoan.setMaturitydate(Long.valueOf(splitted[12]));
                    return new KeyValue<>(splitted[4], cashLoan);
                }).to("cashLoanFormattedStream-topic", Produced.with(Serdes.String(), new CashLoanSerde()));
*/

        cashLoanMainStream.map((key, value) -> {
            String[] splitted = value.split(",");
            System.out.println("Key: " + cashLoan.getExchangecode() + ", " + cashLoan.toString());
            cashLoan.setStatus(Integer.valueOf(splitted[1]));
            cashLoan.setExchangecode(splitted[4]);
            cashLoan.setLoanamount(Double.valueOf(splitted[5]));
            cashLoan.setBranchcode(splitted[7]);
            cashLoan.setValuedate(Long.valueOf(splitted[11]));
            cashLoan.setMaturitydate(Long.valueOf(splitted[12]));
            /*
            try {
                cashLoan.setStatus(Integer.valueOf(splitted[1]));
                cashLoan.setExchangecode(splitted[4]);
                cashLoan.setLoanamount(Double.valueOf(splitted[5]));
                cashLoan.setBranchcode(splitted[7]);
                cashLoan.setValuedate(Long.valueOf(splitted[11]));
                cashLoan.setMaturitydate(Long.valueOf(splitted[12]));
            } catch (Exception e) {
                for (String s : splitted) {
                    System.out.print(s + ",");
                }
                //System.exit(-1);
                throw new ArrayIndexOutOfBoundsException("error occured");
            }
*/
            //System.out.println("Key: " + cashLoan.getExchangecode() + ", " + cashLoan.toString());
            return KeyValue.pair(cashLoan.getExchangecode(), cashLoan);
        }).to("cashLoanFormattedStream-topic", Produced.with(Serdes.String(), new CashLoanSerde()));

        Topology topology = builder.build();
        System.out.println(topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, new StreamsConfigCreator().getConfig());

        kafkaStreams.start();

        //gracefully shutdown
        Runtime.getRuntime().addShutdownHook(new Thread("shutdownHook") {
            @Override
            public void run() {
                kafkaStreams.close();
            }
        });
    }

    public static final class CashLoanSerde extends Serdes.WrapperSerde<CashLoan> {
        public CashLoanSerde() {
            super(new JsonSerializer<CashLoan>(), new JsonDeserializer<CashLoan>(CashLoan.class));
        }
    }
}
