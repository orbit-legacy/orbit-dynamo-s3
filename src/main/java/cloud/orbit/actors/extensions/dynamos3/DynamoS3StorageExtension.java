/*
 Copyright (C) 2016 Electronic Arts Inc.  All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:

 1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
 2.  Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.
 3.  Neither the name of Electronic Arts, Inc. ("EA") nor the names of
     its contributors may be used to endorse or promote products derived
     from this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY ELECTRONIC ARTS AND ITS CONTRIBUTORS "AS IS" AND ANY
 EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL ELECTRONIC ARTS OR ITS CONTRIBUTORS BE LIABLE FOR ANY
 DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package cloud.orbit.actors.extensions.dynamos3;

import com.ea.async.Async;

import org.apache.commons.lang3.SerializationUtils;

import com.amazonaws.AmazonServiceException;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import cloud.orbit.actors.extensions.StorageExtension;
import cloud.orbit.actors.extensions.dynamodb.DynamoDBConfiguration;
import cloud.orbit.actors.extensions.dynamodb.DynamoDBStorageExtension;
import cloud.orbit.actors.extensions.json.ActorReferenceModule;
import cloud.orbit.actors.extensions.s3.S3Configuration;
import cloud.orbit.actors.extensions.s3.S3StorageExtension;
import cloud.orbit.actors.runtime.DefaultDescriptorFactory;
import cloud.orbit.actors.runtime.RemoteReference;
import cloud.orbit.concurrent.Task;
import cloud.orbit.exception.UncheckedException;
import cloud.orbit.util.ExceptionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import static com.ea.async.Async.await;

/**
 * Created by joe@bioware.com on 2016-04-05.
 */
public class DynamoS3StorageExtension implements StorageExtension
{
    static
    {
        //Async.init();
    }

    public static class StateWrapper
    {
        public StateWrapper()
        {

        }

        public StateWrapper(Object state)
        {
            this.state = state;
            isS3Pointer = false;
        }

        public Object state;
        public boolean isS3Pointer;
    }

    private String name = "default";

    private DynamoDBConfiguration dynamoDBConfiguration = new DynamoDBConfiguration();
    private S3Configuration s3Configuration = new S3Configuration();

    private DynamoDBStorageExtension dynamoDBStorageExtension;
    private S3StorageExtension s3StorageExtension;

    private ObjectMapper mapper = new ObjectMapper();

    private String defaultDynamoTableName = "orbit";
    private String s3BucketName = "orbit-bucket";

    public DynamoS3StorageExtension()
    {

    }

    public DynamoS3StorageExtension(final DynamoDBStorageExtension dynamoDBStorageExtension, final S3StorageExtension s3StorageExtension)
    {
        this.dynamoDBStorageExtension = dynamoDBStorageExtension;
        this.s3StorageExtension = s3StorageExtension;
    }

    public DynamoS3StorageExtension(final DynamoDBConfiguration dynamoDBConfiguration, final S3Configuration s3Configuration)
    {
        this.dynamoDBConfiguration = dynamoDBConfiguration;
        this.s3Configuration = s3Configuration;
    }

    @Override
    public Task<Void> start()
    {
        mapper = new ObjectMapper();

        mapper.registerModule(new ActorReferenceModule(DefaultDescriptorFactory.get()));

        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        if(dynamoDBStorageExtension == null)
        {
            dynamoDBStorageExtension = new DynamoDBStorageExtension(dynamoDBConfiguration);
            dynamoDBStorageExtension.setDefaultTableName(defaultDynamoTableName);
        }

        if(s3StorageExtension == null)
        {
            s3StorageExtension = new S3StorageExtension(s3Configuration);
            s3StorageExtension.setBucketName(s3BucketName);
        }

        return Task.allOf(
                dynamoDBStorageExtension.start(),
                s3StorageExtension.start()
        );
    }

    @Override
    public Task<Void> stop()
    {
        return Task.allOf(
                dynamoDBStorageExtension.stop(),
                s3StorageExtension.stop()
        );
    }


    @Override
    public Task<Void> clearState(final RemoteReference<?> reference, final Object state)
    {
        final StateWrapper wrapper = new StateWrapper(state);

        final Boolean readRecord = dynamoDBStorageExtension.readState(reference, wrapper).join();

        List<Task> tasks = new ArrayList<>();

        tasks.add(dynamoDBStorageExtension.clearState(reference, wrapper));

        if(wrapper.isS3Pointer)
        {
            tasks.add(s3StorageExtension.clearState(reference, state));
        }

        return Task.allOf(tasks);
    }

    @Override
    public Task<Boolean> readState(final RemoteReference<?> reference, final Object state)
    {
        final StateWrapper wrapper = new StateWrapper(state);

        final Boolean readRecord = dynamoDBStorageExtension.readState(reference, wrapper).join();

        if(readRecord)
        {
            if(wrapper.isS3Pointer)
            {
                return s3StorageExtension.readState(reference, state);
            }
            else
            {
                try
                {
                    mapper.readerForUpdating(state).readValue(mapper.writeValueAsString(wrapper.state));
                }
                catch(Exception e)
                {
                    throw new UncheckedException(e);
                }

            }
         }

        return Task.fromValue(readRecord);
    }

    @Override
    public Task<Void> writeState(final RemoteReference<?> reference, final Object state)
    {
        final StateWrapper wrapper = new StateWrapper(state);

        try
        {
            dynamoDBStorageExtension.writeState(reference, wrapper).join();

            // Record is fine, we're done
            return Task.done();
        }
        catch(CompletionException e)
        {
            // Was this because of the size of the record?
            if(e.getCause() instanceof AmazonServiceException)
            {
                AmazonServiceException ase = (AmazonServiceException) e.getCause();
                final String errorCode = ase.getErrorCode();
                if(!errorCode.equals("ValidationException"))
                {
                    throw e;
                }
            }

        }

        // If we got here, we must be too big
        wrapper.isS3Pointer = true;
        wrapper.state = null;

        // Write out record to S3 and pointer to Dynamo
        Task s3Write = s3StorageExtension.writeState(reference, state);
        Task dynamoWrite = dynamoDBStorageExtension.writeState(reference, wrapper);

        return Task.allOf(s3Write, dynamoWrite);
    }

    public void setName(final String name)
    {
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    public DynamoDBConfiguration getDynamoDBConfiguration()
    {
        return dynamoDBConfiguration;
    }

    public void setDynamoDBConfiguration(final DynamoDBConfiguration dynamoDBConfiguration)
    {
        this.dynamoDBConfiguration = dynamoDBConfiguration;
    }

    public S3Configuration getS3Configuration()
    {
        return s3Configuration;
    }

    public void setS3Configuration(final S3Configuration s3Configuration)
    {
        this.s3Configuration = s3Configuration;
    }

    public String getDefaultDynamoTableName()
    {
        return defaultDynamoTableName;
    }

    public void setDefaultDynamoTableName(final String defaultDynamoTableName)
    {
        this.defaultDynamoTableName = defaultDynamoTableName;
    }

    public String getS3BucketName()
    {
        return s3BucketName;
    }

    public void setS3BucketName(final String s3BucketName)
    {
        this.s3BucketName = s3BucketName;
    }
}
