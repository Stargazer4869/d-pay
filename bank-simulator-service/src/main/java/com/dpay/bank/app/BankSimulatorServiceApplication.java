package com.dpay.bank.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.dpay.bank", "com.dpay.common"})
public class BankSimulatorServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(BankSimulatorServiceApplication.class, args);
    }
}
