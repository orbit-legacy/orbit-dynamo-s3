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

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cloud.orbit.actors.extensions.dynamodb.DynamoDBConfiguration;
import cloud.orbit.actors.extensions.dynamodb.DynamoDBStorageExtension;
import cloud.orbit.actors.extensions.dynamodb.DynamoDBUtils;
import cloud.orbit.actors.runtime.RemoteReference;
import cloud.orbit.exception.UncheckedException;

import java.io.IOException;

public class DynamoS3DynamoStorageExtension extends DynamoDBStorageExtension
{
    public DynamoS3DynamoStorageExtension()
    {
        super();
    }

    public DynamoS3DynamoStorageExtension(final DynamoDBConfiguration dynamoDBConfiguration)
    {
        super(dynamoDBConfiguration);
    }

    @Override
    protected Item generatePutItem(
            final RemoteReference<?> reference,
            final Object state,
            final Class<?> stateClass,
            final String itemId,
            final ObjectMapper mapper)
    {
        if (state instanceof DynamoS3StorageExtension.StateWrapper)
        {
            try
            {
                DynamoS3StorageExtension.StateWrapper wrapper = (DynamoS3StorageExtension.StateWrapper) state;

                final Item item = super.generatePutItem(reference, wrapper.state, stateClass, itemId, mapper);

                if (wrapper.s3Location != null)
                {
                    final String serializedS3Location = mapper.writeValueAsString(wrapper.s3Location);
                    item.withJSON(DynamoDBS3Utils.FIELD_NAME_S3_LOCATION, serializedS3Location);
                    item.removeAttribute(DynamoDBUtils.FIELD_NAME_DATA);
                }

                return item;
            }
            catch (JsonProcessingException e)
            {
                throw new UncheckedException(e);
            }
        }
        else
        {
            return super.generatePutItem(reference, state, stateClass, itemId, mapper);
        }
    }

    @Override
    protected void readStateInternal(
            final Object state,
            final Class<?> stateClass,
            final Item item,
            final ObjectMapper mapper)
    {
        if (state instanceof DynamoS3StorageExtension.StateWrapper)
        {
            try
            {
                final DynamoS3StorageExtension.StateWrapper stateWrapper = (DynamoS3StorageExtension.StateWrapper) state;
                final String s3Overload = item.getJSON(DynamoDBS3Utils.FIELD_NAME_S3_LOCATION);
                if (s3Overload != null)
                {
                    S3Location s3Location = new S3Location();
                    mapper.readerForUpdating(s3Location).readValue(s3Overload);
                    stateWrapper.s3Location = s3Location;
                }
                else
                {
                    stateWrapper.s3Location = null;
                    super.readStateInternal(stateWrapper.state, stateClass, item, mapper);
                }
            }
            catch (IOException e)
            {
                throw new UncheckedException(e);
            }
        }
        else
        {
            super.readStateInternal(state, stateClass, item, mapper);
        }
    }
}
