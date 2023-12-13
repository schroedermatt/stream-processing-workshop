package org.improving.workshop.simple;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.Random;

@SuppressWarnings("InfiniteLoopStatement")
public class TicketConfirmationConsumer {
    private static final int FAILURE_RATE = 25;
    private static final Random random = new Random();

    public static void main(String[] args) {
        // define the properties for the Kafka consumer
        Properties props = new Properties();

        // where does the consumer connect to Kafka (the "bootstrap server(s)")?
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");

        // what types of data will the consumer receive for the key and value of the records?
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());

        // what group id should Kafka use to identify this consumer?
        // this allows the consumer to keep track of its progress and also have other consumers process in parallel
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ticket-confirmation-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        // create a Kafka consumer instance with the properties above
        KafkaConsumer<String, PurchaseEventTicket.EventTicketConfirmation> consumer = new KafkaConsumer<>(props);

        // subscribe the consumer to the topic(s) it will be consuming from
        consumer.subscribe(List.of(PurchaseEventTicket.OUTPUT_TOPIC));

        // start polling for messages
        while (true) {
            ConsumerRecords<String, PurchaseEventTicket.EventTicketConfirmation> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, PurchaseEventTicket.EventTicketConfirmation> record : records) {
                System.out.println(
                        "RECORD RECEIVED: " +
                                "key = " + record.key() + ", " +
                                "value = " + record.value() + ", " +
                                "partition = " + record.partition() + ", " +
                                "offset = " + record.offset()
                );

                sendMail(record.value());

                // process the received message here
            }

            // commit offsets of the processed records (keeping track of what the consumer has already seen)
            consumer.commitSync();
        }
    }

    @SneakyThrows
    private static void sendMail(PurchaseEventTicket.EventTicketConfirmation eventTicketConfirmation) {
        // todo - figure out how to create new span here from TracerProvider.getTracer(instrumentationName).etc...
        // Mailtrap SMTP server settings
        String host = "sandbox.smtp.mailtrap.io";
        int port = 587;
        String username = "";
        String password = "";

        // Sender and recipient email addresses
        String senderEmail = "tickets@utopia.com";
        String recipientEmail = eventTicketConfirmation.getTicketRequest().customerid() + "@email.com";

        // Email properties
        Properties properties = new Properties();
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.smtp.host", host);
        properties.put("mail.smtp.port", port);

        // Create a session with authentication
        Session session = Session.getInstance(properties, new javax.mail.Authenticator() {
            protected javax.mail.PasswordAuthentication getPasswordAuthentication() {
                return new javax.mail.PasswordAuthentication(username, password);
            }
        });

        try {
            // Generate a random number between 1 and FAILURE_RATE
            int randomValue = random.nextInt(10) + 1;

            // Check if the random value is 1 (indicating a failure)
            if (randomValue == 6) {
                // Simulate a failure
                throw new MessagingException("Whoops, I can't send the email right now.");
            } else if (randomValue == 7) {
                System.out.println("I'm feeling sleepy.");
                Thread.sleep(1000);
            }

            // Create a MimeMessage object
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(senderEmail));
            message.setRecipient(Message.RecipientType.TO, new InternetAddress(recipientEmail));
            message.setSubject("Ticket Confirmation (" + eventTicketConfirmation.getConfirmationId() + ") - Event " + eventTicketConfirmation.getTicketRequest().eventid());
            message.setText("Your ticket has been " + eventTicketConfirmation.getConfirmationStatus() + "!");

            // Send the email
            Transport.send(message);
            System.out.println("Email sent successfully!");
            Span.current().setStatus(StatusCode.OK, "Email Success");
        } catch (MessagingException e) {
            Span.current().setStatus(StatusCode.ERROR, "Email Failure");
            e.printStackTrace();
        }
    }
}
