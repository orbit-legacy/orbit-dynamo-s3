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

package cloud.orbit.actors.extensions.dynamos3.test;

import org.junit.Assume;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.fasterxml.jackson.core.JsonProcessingException;

import cloud.orbit.actors.Actor;
import cloud.orbit.actors.Stage;
import cloud.orbit.actors.extensions.ActorExtension;
import cloud.orbit.actors.extensions.dynamodb.DynamoDBConfiguration;
import cloud.orbit.actors.extensions.dynamodb.DynamoDBConnection;
import cloud.orbit.actors.extensions.dynamodb.DynamoDBUtils;
import cloud.orbit.actors.extensions.dynamos3.DynamoDBS3Utils;
import cloud.orbit.actors.extensions.dynamos3.DynamoS3StorageExtension;
import cloud.orbit.actors.extensions.dynamos3.S3Location;
import cloud.orbit.actors.extensions.s3.S3Configuration;
import cloud.orbit.actors.test.StorageBaseTest;
import cloud.orbit.actors.test.StorageTest;
import cloud.orbit.actors.test.StorageTestState;
import cloud.orbit.exception.UncheckedException;
import cloud.orbit.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public abstract class BaseDynamoDBS3PersistenceTest extends StorageBaseTest
{
    protected static final String DEFAULT_TABLE_NAME = "orbit-ci-test";
    protected static final String TEST_STRING_LONG = new String(new char[3000000]).replace("\0", "X");

    private DynamoDBConnection dynamoDBConnection;
    private DynamoDBConfiguration dynamoDBConfiguration;
    private DynamoS3StorageExtension dynamoS3StorageExtension;
    private S3Configuration s3Configuration;

    public BaseDynamoDBS3PersistenceTest()
    {
        Assume.assumeTrue(!StringUtils.equals(System.getenv("TRAVIS"), "true")
                || StringUtils.equals(System.getenv("ORBIT_TEST_DYNAMOS3_ENABLED"), "true"));

        String awsRegion = System.getenv("AWS_DEFAULT_REGION");
        if (StringUtils.isBlank(awsRegion))
        {
            awsRegion = "us-west-2";
        }

        dynamoDBConfiguration = new DynamoDBConfiguration.Builder()
                .withCredentialType(cloud.orbit.actors.extensions.dynamodb.AmazonCredentialType.DEFAULT_PROVIDER_CHAIN)
                .withRegion(awsRegion)
                .build();

        s3Configuration = new S3Configuration.Builder()
                .withCredentialType(cloud.orbit.actors.extensions.s3.AmazonCredentialType.DEFAULT_PROVIDER_CHAIN)
                .withRegion(awsRegion)
                .build();

        getStorageExtension();
    }

    @Override
    public Class<? extends StorageTest> getActorInterfaceClass()
    {
        return getActorInterfaceClassAs();
    }

    public <X> Class<X> getActorInterfaceClassAs()
    {
        return (Class<X>) getTestingActorInterfaceClass();
    }

    public abstract Class<?> getTestingActorInterfaceClass();
    public abstract Class<?> getTestingActorStateClass();

    @Override
    public ActorExtension getStorageExtension()
    {
        if (dynamoS3StorageExtension == null)
        {
            dynamoS3StorageExtension = new DynamoS3StorageExtension(dynamoDBConfiguration, s3Configuration);

            final String bucketName = System.getenv("ORBIT_TEST_S3_BUCKET");
            if (StringUtils.isNotBlank(bucketName))
            {
                dynamoS3StorageExtension.setS3BucketName(bucketName);
            }

            final String tableName = System.getenv("ORBIT_TEST_DYNAMO_TABLE");
            if (StringUtils.isNotBlank(tableName))
            {
                dynamoS3StorageExtension.setDefaultDynamoTableName(tableName);
            }

            dynamoS3StorageExtension.setDefaultDynamoTableName(DEFAULT_TABLE_NAME);
        }

        return dynamoS3StorageExtension;
    }

    @Override
    public void initStorage()
    {
        dynamoDBConnection = new DynamoDBConnection(dynamoDBConfiguration);

        closeStorage();
    }

    @Override
    public void closeStorage()
    {
        try
        {
            dynamoDBConnection.getDynamoClient().describeTable(getTableName());
            dynamoDBConnection.getDynamoClient().deleteTable(getTableName());
            waitForTableStatusOtherThan(getTableName(), TableStatus.DELETING.toString());
        }
        catch (ResourceNotFoundException e)
        {

        }
    }

    public long count(Class<? extends StorageTest> actorInterface)
    {
        final String tableName = getTableName();
        try
        {
            final Table table = dynamoDBConnection.getDynamoDB().getTable(tableName);

            // if we get here, the table exists, it may not be ready yet.
            ensureTableIsActive(tableName);
            ScanSpec scanSpec = new ScanSpec()
                    .withAttributesToGet(
                            DynamoDBUtils.FIELD_NAME_PRIMARY_ID,
                            DynamoDBUtils.FIELD_NAME_OWNING_ACTOR_TYPE)
                    .withConsistentRead(true);

            final ItemCollection<ScanOutcome> queryResults = table.scan(scanSpec);

            int count = 0;
            Iterator<Item> iterator = queryResults.iterator();

            while (iterator.hasNext())
            {
                Item item = iterator.next();
                if (actorInterface.getName().equals(item.getString(DynamoDBUtils.FIELD_NAME_OWNING_ACTOR_TYPE)))
                {
                    count++;
                }
            }

            return count;
        }
        catch (ResourceNotFoundException e)
        {
            // Assumption: nonexistent table is deliberate (count could be called before a get/put which
            // creates the table), meaning it has no entries
            return 0;
        }
    }

    @Override
    public StorageTestState readState(final String identity)
    {
        final Table table = dynamoDBConnection.getDynamoDB().getTable(getTableName());
        final Item item = table.getItem("_id", generateItemId(identity));

        if (item != null)
        {
            try
            {
                final StorageTestState testState = new HelloState();
                dynamoDBConnection.getMapper().readerForUpdating(testState).readValue(item.getJSON(DynamoDBUtils.FIELD_NAME_DATA));
                return testState;
            }
            catch (Exception e)
            {
                throw new UncheckedException(e);
            }
        }
        return null;
    }

    public boolean isStateFieldNull(final String identity)
    {
        final Table table = dynamoDBConnection.getDynamoDB().getTable(getTableName());
        final Item item = table.getItem("_id", generateItemId(identity));

        if (item != null)
        {
            return !item.hasAttribute(DynamoDBUtils.FIELD_NAME_DATA);
        }
        else
        {
            throw new IllegalArgumentException(identity);
        }
    }

    public String readOwningType(final String identity)
    {
        final Table table = dynamoDBConnection.getDynamoDB().getTable(getTableName());
        final Item item = table.getItem("_id", generateItemId(identity));

        if (item != null)
        {
            return item.getString(DynamoDBUtils.FIELD_NAME_OWNING_ACTOR_TYPE);
        }
        return null;
    }

    public S3Location readS3Location(final String identity)
    {
        final Table table = dynamoDBConnection.getDynamoDB().getTable(getTableName());
        final Item item = table.getItem("_id", generateItemId(identity));

        if (item != null)
        {
            try
            {
                S3Location s3Location = new S3Location();
                dynamoDBConnection.getMapper().readerForUpdating(s3Location).readValue(item.getJSON(DynamoDBS3Utils.FIELD_NAME_S3_LOCATION));
                return s3Location;
            }
            catch (IOException e)
            {
                throw new UncheckedException(e);
            }
        }
        return null;
    }

    protected abstract String getTableName();
    protected abstract String generateItemId(final String identity);

    public long count()
    {
        return count(getActorInterfaceClass());
    }

    @Override
    public int heavyTestSize()
    {
        return 100;
    }

    @Test
    public void testOwningType()
    {
        Stage stage = this.createStage();

        final String actorId = "sampleData";
        final Hello helloActor = Actor.getReference(getActorInterfaceClassAs(), actorId);

        helloActor.setSampleData(new HelloDto()).join();
        assertEquals(getActorInterfaceClass().getName(), readOwningType(actorId));
    }

    @Test
    public void testPersistingNullValues() throws Exception
    {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName(null);
        sampleData.setNameList(null);
        sampleData.setNameProperties(null);
        sampleData.setNameSet(null);
        sampleData.setByteArray(null);

        testSampleData(sampleData);
    }

    @Test
    public void testPersistingNullContainedValues() throws Exception
    {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName(null);

        List<String> nameList = new ArrayList<>();
        nameList.add(null);

        sampleData.setNameList(nameList);

        final Map<String, Object> nameProperties = new HashMap<>();
        nameProperties.put("Jim", null);

        sampleData.setNameProperties(nameProperties);
        sampleData.setNameSet(new HashSet<>(Collections.singletonList(null)));

        testSampleData(sampleData);
    }

    @Test
    public void testPersistingEmptyContainers() throws Exception
    {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName(null);
        sampleData.setNameList(new ArrayList<>());
        sampleData.setNameProperties(new HashMap<>());
        sampleData.setNameSet(new HashSet<>());
        sampleData.setByteArray(new byte[0]);

        testSampleData(sampleData);
    }

    @Test
    public void testPersistingEmptyStringValues() throws Exception
    {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName("");

        List<String> nameList = new ArrayList<>();
        nameList.add("");

        sampleData.setNameList(nameList);

        final Map<String, Object> nameProperties = new HashMap<>();
        nameProperties.put("Jim", "");

        sampleData.setNameProperties(nameProperties);
        sampleData.setNameSet(new HashSet<>(Collections.singletonList("")));

        testSampleData(sampleData);
    }

    @Test
    public void testPersistingNonBlankNonEmptyValues() throws Exception
    {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName("Larry");

        final Map<String, Object> nameProperties = new HashMap<>();
        nameProperties.put("Curly", "one");
        nameProperties.put("Larry", 2);
        nameProperties.put("Moe", "three".getBytes());

        sampleData.setNameProperties(nameProperties);

        sampleData.setNameList(new ArrayList<>(nameProperties.keySet()));
        sampleData.setNameSet(new HashSet<>(nameProperties.keySet()));

        sampleData.setByteArray(sampleData.getName().getBytes());

        testSampleData(sampleData);
    }

    @Test
    public void testPersistingBigData()
    {
        Stage stage = this.createStage();

        final Hello helloActor = Actor.getReference(getActorInterfaceClassAs(), "sampleData");
        helloActor.sayHello(TEST_STRING_LONG).join();

        assertTrue(isStateFieldNull("sampleData"));
        assertNotNull(readS3Location("sampleData"));
    }

    @Test
    public void testActorHandlesBigData()
    {
        final HelloDto sampleData = new HelloDto();
        sampleData.setName(TEST_STRING_LONG);

        Stage stage = this.createStage();

        final Hello helloActor = Actor.getReference(getActorInterfaceClassAs(), "sampleData");

        helloActor.setSampleData(sampleData).join();

        final HelloDto loadedSampleData = helloActor.getSampleData(true).join();

        assertEquals(TEST_STRING_LONG, loadedSampleData.getName());
    }

    private void testSampleData(HelloDto sampleData) throws JsonProcessingException
    {
        Stage stage = this.createStage();

        final Hello helloActor = Actor.getReference(getActorInterfaceClassAs(), "sampleData");

        helloActor.setSampleData(sampleData).join();
        final HelloDto loadedSampleData = helloActor.getSampleData(true).join();

        jsonEquals(sampleData, loadedSampleData);
    }

    protected void jsonEquals(Object expect, Object actual)
    {
        try
        {
            assertEquals(
                    dynamoDBConnection.getMapper().writeValueAsString(expect),
                    dynamoDBConnection.getMapper().writeValueAsString(actual));
        }
        catch (JsonProcessingException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void waitForTableStatusOtherThan(final String tableName, final String tableState)
    {
        try
        {
            while (true)
            {
                final DescribeTableResult describe = dynamoDBConnection.getDynamoClient().describeTable(tableName);
                if (!describe.getTable().getTableStatus().equals(tableState))
                {
                    return;
                }

                Thread.sleep(500);
            }

        }
        catch (InterruptedException e)
        {
            throw new UncheckedException(e);
        }
    }

    protected void ensureTableIsActive(final String tableName)
    {
        try
        {
            while (true)
            {
                final DescribeTableResult describe = dynamoDBConnection.getDynamoClient().describeTable(tableName);
                if (describe.getTable().getTableStatus().equals(TableStatus.ACTIVE.name()))
                {
                    return;
                }

                Thread.sleep(500);
            }

        }
        catch (InterruptedException e)
        {
            throw new UncheckedException(e);
        }
    }
}
