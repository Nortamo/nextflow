/*
 * Copyright 2020-2021, Seqera Labs
 * Copyright 2013-2019, Centre for Genomic Regulation (CRG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.executor

import java.nio.file.Path
import java.util.regex.Pattern

import groovy.util.logging.Slf4j
import groovy.transform.InheritConstructors

import nextflow.processor.TaskRun
/**                                                    
 * Processor for HyperQueue execution engine   
 *                                                     
 * https://it4innovations.github.io/hyperqueue/stable/          
 *                                                       
 * @author Henrik Nortamo <henrik.nortamo@csc.fi>
 */                                                    
@Slf4j
class HqExecutor extends AbstractGridExecutor {


    static private Pattern SUBMIT_REGEX = ~/Job submitted successfully, job ID: (\d+)/
    private String SERVER_DIR_ARG 

    @Override           
    void register() {   
        // We don't want to default to the user home directory
        super.register()
        String  HqServerDir= System.getenv("HQ_SERVER_DIR") ?: session.getExecConfigProp('hq','serverDir',null)  
        if ( HqServerDir == null ) {
            throw new Exception("No HyperQueue server directory set")
        } 

        SERVER_DIR_ARG = "--server-dir=${HqServerDir}"    
        log.debug "Using HyperQueue server directory ${HqServerDir}"
    }

    /**
     * Gets the directives to submit the specified task to the cluster for execution
     *
     * @param task A {@link TaskRun} to be submitted
     * @param result The {@link List} instance to which add the job directives
     * @return A {@link List} containing all directive tokens and values.
     */
    protected List<String> getDirectives(TaskRun task, List<String> result) {

        result << "--cwd=${quote(task.workDir)}" << '' 
        result << "--name=${getJobNameFor(task)}" << ''  
        result << "--stdout=${quote(task.workDir.resolve(TaskRun.CMD_OUTFILE))}" << '' 
        result << "--stderr=${quote(task.workDir.resolve(TaskRun.CMD_ERRFILE))}" << ''

        if( task.config.cpus > 1 ) {
            result << "--cpus=${task.config.cpus.toString()}"  << '' 
        }
        else {

            result << '--cpus=1' << '' 
        }
       
        if( task.config.getMemory() ) {                                           
            // No enforcement, Hq just makes sure that the allocated value is below the limit
            result << '--resource mem=' + task.config.getMemory().toMega().toString() + 'M' << ''
        }                                                                         

        if ( task.config.accelerator ) {
            result << "--resource gpus=${task.config.accelerator.limit}" << ''
        }
        
        // Bind processes to assigned cpu cores 
        result << '--pin' << '' 

        if( task.config.time ) {
            List<String> baseTime=task.config.getTime().format('HH:mm:ss').split(':')
            String humanTime="${baseTime[0]}h ${baseTime[1]}m ${baseTime[2]}s"
            result << "--time-limit=\"${humanTime}\"" << ''
        }

        if( task.config.clusterOptions ) {
            result << task.config.clusterOptions.toString() << ''
        }

        return result
    }

    String getHeaderToken() { '#HQ' }

    /**
     * The command line to submit this job
     *
     * @param task The {@link TaskRun} instance to submit for execution to the cluster
     * @param scriptFile The file containing the job launcher script
     * @return A list representing the submit command line
     */
    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile ) {

        ['hq',SERVER_DIR_ARG,'submit','--directives=file', task.workDir.resolve(scriptFile.getName())]

    }

    /**
     * Parse the string returned by the {@code hq} command and extract the job ID string
     *
     * @param text The string returned when submitting the job
     * @return The actual job ID string
     */
    @Override
    def parseJobId(String text) {

        for( String line : text.readLines() ) {
            def m = SUBMIT_REGEX.matcher(line)
            if( m.find() ) {
                return m.group(1).toString()
            }
        }

        throw new IllegalStateException("Invalid HQ submit response:\n$text\n\n")
    }

    @Override
    protected List<String> getKillCommand() { ['hq',SERVER_DIR_ARG,'job','cancel'] }

    // JobIds to the cancel command need to be separated by a comma 
    @Override
    protected List<String> killTaskCommand(def jobId) { 
        final result = getKillCommand()                 
        if( jobId instanceof Collection ) {             
            result.add(jobId.join(','))
            log.trace "Kill command: ${result}"         
        }                                               
        else {                                          
            result.add(jobId.toString())                
        }                                               
        return result                                   
    }                                                   


    @Override
    protected List<String> queueStatusCommand(Object queue) {
        final result = ['hq',SERVER_DIR_ARG,'job','list','--all']
        return result
    }

    /*
     *  Maps HQ job status to nextflow status
     */
    static private Map STATUS_MAP = [
            'RUNNING': QueueStatus.RUNNING,
            'FINISHED': QueueStatus.DONE,
            'CANCELED': QueueStatus.ERROR, 
            'FAILED': QueueStatus.ERROR,
            'WAITING': QueueStatus.PENDING,
    ]

    @Override
    protected Map<String, QueueStatus> parseQueueStatus(String text) {

        def result = [:]
        // The job listing contains a lot of pretty borders and headers
        // Which we need to remove
        def job_list = text.split('\n')[4..-2].collect{ x -> x.split('\\|')[1,3].join(' ').trim()}.join('\n')
        job_list.eachLine { String line ->
            def cols = line.split(/\s+/)
            if( cols.size() == 2 ) {
                result.put( cols[0], STATUS_MAP.get(cols[1]) )
            }
            else {
                log.debug "[HQ] invalid status line: `$line`"
            }
        }

        return result
    }

    final protected BashWrapperBuilder createBashWrapperBuilder(TaskRun task) {
        final builder = new HqWrapperBuilder(task)
        builder.headerScript = getHeaders(task)
        return builder
    }

    @InheritConstructors
    static class HqWrapperBuilder extends BashWrapperBuilder {
        Path build() {
            final wrapper = super.build()
            // give execute permission to wrapper file
            wrapper.setExecutable(true)
            return wrapper
        }

    }
}
