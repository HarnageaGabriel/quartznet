#region License

/*
 * All content copyright Marko Lahma, unless otherwise indicated. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

#endregion

using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

using Quartz.Extensibility;
using Quartz.Impl;
using Quartz.Impl.AdoJobStore;
using Quartz.Jobs;

namespace Quartz.Tests.Integration.Impl.AdoJobStore;

[NonParallelizable]
public class AdoJobStoreTriggerAcquisitionFilterTest
{
    [Test]
    [Category("db-sqlite")]
    public async Task ShouldExcludeConfiguredJobTypeFromTriggerAcquisition()
    {
        string dbFileName = $"test-acquisition-filter-{Guid.NewGuid():N}.db";
        string connectionString = $"Data Source={dbFileName};";
        IScheduler scheduler = null;

        try
        {
            await using (SqliteConnection connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();
                await using SqliteCommand command = new SqliteCommand(LoadSqliteTableScript(), connection);
                await command.ExecuteNonQueryAsync();
            }

            scheduler = await CreateScheduler(connectionString);
            await scheduler.Clear();

            IJobDetail excludedJob = JobBuilder.Create<NoOpJob>()
                .WithIdentity("excluded-job")
                .Build();
            ITrigger excludedTrigger = TriggerBuilder.Create()
                .WithIdentity("excluded-trigger")
                .ForJob(excludedJob)
                .StartNow()
                .Build();

            IJobDetail includedJob = JobBuilder.Create<NativeJob>()
                .WithIdentity("included-job")
                .Build();
            ITrigger includedTrigger = TriggerBuilder.Create()
                .WithIdentity("included-trigger")
                .ForJob(includedJob)
                .StartNow()
                .Build();

            await scheduler.ScheduleJob(excludedJob, excludedTrigger);
            await scheduler.ScheduleJob(includedJob, includedTrigger);

            (await scheduler.GetTriggerState(excludedTrigger.Key)).Should().Be(TriggerState.Normal);
            (await scheduler.GetTriggerState(includedTrigger.Key)).Should().Be(TriggerState.Normal);

            PagedResult<JobHeader> jobs = await scheduler.QueryJobs(new JobQuery());
            string excludedJobTypeName = jobs.Items.Single(x => x.Key == excludedJob.Key).JobTypeName;

            IJobStore jobStore = ((StdScheduler)scheduler).scheduler.resources.JobStore;
            List<IOperableTrigger> acquired = await jobStore.AcquireNextTriggers(new TriggerAcquisitionRequest
            {
                NoLaterThan = DateTimeOffset.UtcNow.AddSeconds(5),
                MaxCount = 2,
                JobTypesToExclude = [excludedJobTypeName]
            });

            acquired.Should().ContainSingle();
            acquired.Single().Key.Should().Be(includedTrigger.Key);
        }
        finally
        {
            if (scheduler is not null)
            {
                await scheduler.Clear();
                await scheduler.Shutdown();
            }

            SqliteConnection.ClearAllPools();
            if (File.Exists(dbFileName))
            {
                File.Delete(dbFileName);
            }
        }
    }

    private static async ValueTask<IScheduler> CreateScheduler(
        string connectionString,
        CancellationToken cancellationToken = default)
    {
        string suffix = Guid.NewGuid().ToString("N");

        QuartzSchedulerBuilder config = QuartzSchedulerBuilder.Create();
        config.ConfigureScheduler(options =>
        {
            options.InstanceId = $"acquisition_filter_instance_{suffix}";
            options.InstanceName = $"AcquisitionFilterScheduler_{suffix}";
        });

        config.UsePersistentStore(store =>
        {
            store.Configure(options =>
            {
                options.UseProperties = false;
                options.PerformSchemaValidation = true;
            });

            store.UseGenericDatabase("SQLite-Microsoft", connectionString);
            store.Services.Replace(ServiceDescriptor.Singleton(typeof(IDriverDelegate), typeof(SQLiteDelegate)));
            store.UseSystemTextJsonSerializer();
        });

        return await config.BuildScheduler(cancellationToken);
    }

    private static string LoadSqliteTableScript()
    {
        string path = File.Exists("../../../../database/tables/tables_sqlite.sql")
            ? "../../../../database/tables/tables_sqlite.sql"
            : "../../../../../database/tables/tables_sqlite.sql";

        return File.ReadAllText(path);
    }
}
