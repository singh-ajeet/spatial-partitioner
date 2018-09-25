package com.pb.spectrum.spatial.partitioner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public final class ApplicationOptionsParser {
        private static final Log LOG = LogFactory.getLog(ApplicationOptionsParser.class);

    private static final String GENERIC_OPTIONS = IOUtils.LINE_SEPARATOR +
            "Generic options supported are" + IOUtils.LINE_SEPARATOR +
            " -conf <configuration file>     specify an application configuration file using hadoop configuration format" + IOUtils.LINE_SEPARATOR +
            " -D <property=value>            specify a value for given property" + IOUtils.LINE_SEPARATOR +
            IOUtils.LINE_SEPARATOR +
            "Additional options for 'hadoop' and 'spark-submit' may be specified as documented for those applications.";

    private static final Pattern DEPRECATED_CONFIG_OPTION = Pattern.compile("-config(?=\\s*$|=)");

    private final String m_commandText;
    private final Set<String> m_requiredOptions;
    private final Configuration m_config;

    public ApplicationOptionsParser(String commandText, Configuration baseConfig, String[] args, Collection<Option> options, boolean isConfigOptionRequired) throws ParseException {
        m_commandText = commandText;
        m_requiredOptions = new HashSet<>();

        for(Option option : options) {
            int argCount = option.getArgs();
            if(argCount != Option.UNINITIALIZED && (argCount < 0 || argCount > 1)){
                throw new IllegalStateException("Current parser implementation supports exactly 0 or 1 argument values. " +
                        "Consider using a single comma separated value to represent multiple arguments.  Otherwise, add support and remove this error.");
            }
            if (option.isRequired()) {
                markRequired(option);
            }
        }

        // in order to handle the deprecated -config arg, we'll silently change -config to -conf
        for(int i=0; i<args.length; i++){
            args[i] = DEPRECATED_CONFIG_OPTION.matcher(args[i]).replaceFirst("-conf");
        }

        GenericOptionsParser genericParser;
        CommandLine commandLine;
        try {
            // use GenericOptionsParser to get hadoop supported properties as well as our own
            try {
                genericParser = new GenericOptionsParser(baseConfig, buildOptions(options, false), args);
            } catch (IOException e) {
                LOG.error("Error parsing input", e);
                throw new IllegalStateException("Error parsing input: " + e.getMessage(), e);
            }

            m_config = genericParser.getConfiguration();
            commandLine = genericParser.getCommandLine();
            /* there was a parsing error and the error was simply logged. and we only catch IOExceptions
            * Thus this goes unnoticed until we try to use the commandLine object, which is null.*/
            if(commandLine == null){
                throw new ParseException("options parsing failed");
            }

            // we want to fail on seeing an unrecognized property... GenericOptionsParser doesn't support this
            // directly, but, since we send it all of our supported options, then really, we should have no
            // remaining options.  if there are remaining options, assume they're unsupported
            if(genericParser.getRemainingArgs().length > 0){
                throw new UnrecognizedOptionException("Unrecognized option: " + genericParser.getRemainingArgs()[0]);
            }

/*
            // now, we want to error in case a user specified a conf file that doesn't exist
            String[] confPaths = (String[]) ObjectUtils.defaultIfNull(commandLine.getOptionValues("conf"), new String[]{});
            if(isConfigOptionRequired && confPaths.length < 1){
                throw new MissingOptionException(" -conf option must be specified");
            }

            for(String conf : confPaths){
                // use the same path checking used in GenericOptionsParser
                // this allows both path formats: /path/to/conf.xml and file:///path/to/conf.xml
                if(!new File(new Path(conf).toUri().getPath()).getAbsoluteFile().exists()){
                    throw new IllegalArgumentException("Invalid job configuration path given: " + conf);
                }
            }
*/

            // copy parsed options to the configuration
            List<String> missingOptions = new ArrayList<>();
            for(Option option : options){
                if(commandLine.hasOption(option.getOpt())){
                    if(option.hasArg()) {
                        m_config.set(option.getOpt(), commandLine.getOptionValue(option.getOpt()));
                    } else {
                        m_config.set(option.getOpt(), Boolean.TRUE.toString());
                    }
                }
                if(isRequired(option) && m_config.get(option.getOpt()) == null){
                    missingOptions.add(option.getOpt());
                }
            }
            if(!missingOptions.isEmpty()){
                throw new MissingOptionException(missingOptions);
            }
        } catch (ParseException|IllegalStateException ex){
            LOG.debug(ex);
            System.out.println("ERROR: " + ex.getMessage());
            printUsage(options);
            throw ex;
        } finally {
            // reset the option required state outside the scope of this object
            for(Option option : options) {
                option.setRequired(isRequired(option));
            }
        }
    }

    public Configuration getConfiguration() {
        return m_config;
    }

    private void printUsage(Collection<Option> options){
        // when building the options bundle for help, make sure the required flag is properly set
        HelpFormatter formatter = new HelpFormatter();
        Options helpOptions = buildOptions(options, true);
        formatter.printHelp(140, m_commandText + " [<generic options>]", "", helpOptions, GENERIC_OPTIONS, true);
    }

    private void markRequired(Option option){
        m_requiredOptions.add(option.getOpt());
    }

    private boolean isRequired(Option option){
        return m_requiredOptions.contains(option.getOpt());
    }

    private Options buildOptions(Collection<Option> optionList, boolean forHelp){
        Options options = new Options();
        for(Option option : optionList){
            if(forHelp && isRequired(option)) {
                option.setRequired(true);
            } else {
                option.setRequired(false);
            }
            options.addOption(option);
        }
        return options;
    }
}
