package com.shredder.example.aws.s3.util;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class PropertyReader {

    private static PropertyReader instance;
    private FileBasedConfiguration configuration;

    private PropertyReader() {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class).configure(params.properties().setFileName("application.properties"));
        try {
            configuration = builder.getConfiguration();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }
    public static synchronized PropertyReader getInstance() {
        if (instance == null) {
            instance = new PropertyReader();
        }
        return instance;
    }

    public String getProperty(String key) {
        return (String) configuration.getProperty(key);
    }
}


