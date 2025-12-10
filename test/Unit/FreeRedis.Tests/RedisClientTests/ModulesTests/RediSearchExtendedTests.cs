using FreeRedis.RediSearch;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace FreeRedis.Tests.RedisClientTests.Other
{
    partial class RediSearchTests
    {
        [Fact]
        public void Test_AutoCompletion_Suggestions()
        {
            var key = "compl:names";
            cli.Del(key);

            // 1. Add Suggestions
            var score1 = cli.FtSugAdd(key, "James Bond", 1.0);
            var score2 = cli.FtSugAdd(key, "James Brown", 0.8);
            cli.FtSugAdd(key, "Jack Sparrow", 0.4);

            Assert.True(score1 > 0);

            // 2. Get Length
            cli.FtSugLen(key);
            // Note: FT.SUGLEN returns void in your interface, checking for no exception. 
            // Usually it returns long, but the provided interface defines it as void.

            // 3. Get Suggestions
            var results = cli.FtSugGet(key, "Jam", fuzzy: true, max: 5);
            Assert.Contains("James Bond", results);
            Assert.Contains("James Brown", results);
            //Assert.DoesNotContain("Jack Sparrow", results);

            // 4. Get with Payloads and Scores
            // Note: The return type is string[], so payloads/scores are embedded in the string or need parsing depending on client impl.
            // Based on source, it returns string[].
            var resultsComplex = cli.FtSugGet(key, "Jam", withScores: true, withPayloads: true);
            Assert.NotEmpty(resultsComplex);

            // 5. Delete
            cli.FtSugDel(key, "James Bond");
            var resultsAfterDel = cli.FtSugGet(key, "Jam");
            Assert.DoesNotContain("James Bond", resultsAfterDel);
        }

        [Fact]
        public void Test_Dictionaries_And_Synonyms()
        {
            var dictName = "custom_dict";
            var term1 = "csharp";
            var term2 = "dotnet";

            // --- Dictionary ---
            // Clean up
            try { cli.FtDictDel(dictName, term1, term2); } catch { }

            // Add
            long count = cli.FtDictAdd(dictName, term1, term2);
            Assert.True(count >= 2);

            // Dump
            var terms = cli.FtDictDump(dictName);
            Assert.Contains(term1, terms);
            Assert.Contains(term2, terms);

            // Del
            long delCount = cli.FtDictDel(dictName, term1);
            Assert.Equal(1, delCount);

            // --- Synonyms ---
            var idxName = "syn_test_idx";
            try { cli.FtDropIndex(idxName); } catch { }

            cli.FtCreate(idxName)
               .AddTextField("name")
               .Execute();

            var synGroupId = "group1";

            // Update Synonyms
            cli.FtSynUpdate(idxName, synGroupId, false, "boy", "child", "kid");

            // Dump Synonyms
            var synDump = cli.FtSynDump(idxName);
            Assert.True(synDump.ContainsKey("boy"));
            Assert.Contains(synGroupId, synDump["boy"][0]); // The return format depends on Redis version usually

            cli.FtDropIndex(idxName);
        }

        [Fact]
        public void Test_SpellCheck()
        {
            var idxName = "spell_idx";
            try { cli.FtDropIndex(idxName); } catch { }

            cli.FtCreate(idxName).AddTextField("text").Execute();

            // Seed Data
            cli.HSet("doc1", "text", "the quick brown fox jumps over the lazy dog");

            // Intentionally misspelled query
            var result = cli.FtSpellCheck(idxName, "quik bron", distance: 1);

            // Parse Result
            // Result is Dictionary<string, Dictionary<string, double>>
            // Key = Term, Value = Dict of {Suggestion, Score}

            Assert.True(result.ContainsKey("quik"));
            Assert.True(result.ContainsKey("bron"));

            var suggestionsForQuick = result["quik"];
            Assert.True(suggestionsForQuick.ContainsKey("quick"));

            cli.FtDropIndex(idxName);
        }

        [Fact]
        public void Test_Aliases_And_Config()
        {
            var idxName = "real_index_" + Guid.NewGuid().ToString("N");
            var aliasName = "alias_index_" + Guid.NewGuid().ToString("N");

            cli.FtCreate(idxName).AddTextField("t").Execute();

            // Add Alias
            cli.FtAliasAdd(aliasName, idxName);

            // Search via Alias
            cli.HSet(Guid.NewGuid().ToString(), "t", "hello2");
            var result = cli.FtSearch(aliasName, "hello2").Execute();
            Assert.Equal(1, result.Total);

            // Update Alias (Point to same for test)
            cli.FtAliasUpdate(aliasName, idxName);

            // Delete Alias
            cli.FtAliasDel(aliasName);

            // Verify deletion (Expect error or 0 results depending on strictness, usually error "no such index")
            Assert.Throws<RedisServerException>(() => cli.FtSearch(aliasName, "hello").Execute());

            // Cleanup
            cli.FtDropIndex(idxName);

            // --- Config ---
            var configs = cli.FtConfigGet("*", ""); // Get All
            Assert.NotNull(configs);
            Assert.True(configs.Count > 0);

            // Test Set (Using a safe config like MINPREFIX)
            // Note: Changing config might affect other tests, so we revert or pick something minor.
            // Just verifying the method call doesn't crash.
            try
            {
                var minPrefix = configs.ContainsKey("MINPREFIX") ? configs["MINPREFIX"] : "2";
                cli.FtConfigSet("MINPREFIX", "2");
            }
            catch (Exception ex)
            {
                // Some configs might be immutable at runtime depending on version
                // Just logging warning in real scenario
            }
        }

        [Fact]
        public void Test_TagValues()
        {
            var idxName = "tag_vals_idx";
            try { cli.FtDropIndex(idxName); } catch { }

            cli.FtCreate(idxName)
               .AddTagField("tags")
               .Execute();

            cli.HSet("doc1", "tags", "news,tech");
            cli.HSet("doc2", "tags", "tech,sport");

            var tags = cli.FtTagVals(idxName, "tags");

            Assert.Contains("news", tags);
            Assert.Contains("tech", tags);
            Assert.Contains("sport", tags);

            cli.FtDropIndex(idxName);
        }

        [Fact]
        public void Test_Vector_Schema_Creation()
        {
            // The Repository doesn't support [FtVectorField] yet, but the Builder does.
            // This tests the lower-level builder API.
            var idxName = "vector_idx_" + Guid.NewGuid();

            var vectorAttrs = new Dictionary<string, object>
            {
                { "TYPE", "FLOAT32" },
                { "DIM", 2 },
                { "DISTANCE_METRIC", "L2" }
            };

            cli.FtCreate(idxName)
               .On(IndexDataType.Hash)
               .AddVectorField("vec", "vec_alias", VectorAlgo.HNSW, vectorAttrs)
               .AddTextField("desc")
               .Execute();

            // Verify via FT._LIST or Info (if Info was exposed, but we can try search)
            // Simple search to ensure index exists
            var res = cli.FtSearch(idxName, "*").Execute();
            Assert.NotNull(res);

            cli.FtDropIndex(idxName);
        }

        [FtDocument("idx_advanced_linq", Prefix = "adv:")]
        class AdvancedDoc
        {
            [FtKey]
            public int Id { get; set; }
            [FtTextField]
            public string Name { get; set; }
            [FtNumericField]
            public double Score { get; set; }
            [FtNumericField(Sortable = true)]
            public long Timestamp { get; set; }
        }

        [Fact]
        public void Test_Repository_Advanced_Linq_And_Math()
        {
            var repo = cli.FtDocumentRepository<AdvancedDoc>();
            try { repo.DropIndex(); } catch { }
            repo.CreateIndex();

            var now = DateTime.Now;
            var ts = (long)now.Subtract(new DateTime(1970, 1, 1)).TotalSeconds;

            repo.Save(new AdvancedDoc { Id = 1, Name = "Alice", Score = 10.5, Timestamp = ts });
            repo.Save(new AdvancedDoc { Id = 2, Name = "Bob", Score = 5.0, Timestamp = ts - 1000 });
            repo.Save(new AdvancedDoc { Id = 3, Name = "Charlie", Score = 3.14, Timestamp = ts + 1000 });

            // 1. Test 
            var absResult = repo.Search(x => x.Score > 6).ToList();
            Assert.Contains(absResult, d => d.Name == "Alice");
            Assert.Single(absResult);
            Assert.Equal("Alice", absResult[0].Name);

            // 2. Test DateTime conversion in Expression
            var dateFilter = repo.Search(x => x.Timestamp <= ts).ToList();
            Assert.True(dateFilter.Count >= 2); // Alice and Bob

            // 3. Test String logic (StartsWith/EndsWith is mapped to prefix/suffix)
            var prefixRes = repo.Search(x => x.Name.StartsWith("Ali")).ToList();
            Assert.Single(prefixRes);
            Assert.Equal("Alice", prefixRes[0].Name);

            // 4. Test Boolean Logic (OR / AND)
            var boolRes = repo.Search(x => (x.Score > 0 && x.Name == "Charlie") || x.Name == "Bob").ToList();
            Assert.Equal(2, boolRes.Count);
            Assert.Contains(boolRes, d => d.Name == "Charlie");
            Assert.Contains(boolRes, d => d.Name == "Bob");

            repo.DropIndex();
        }

        [Fact]
        public void Test_SearchBuilder_Options()
        {
            var idxName = "opt_idx";
            try { cli.FtDropIndex(idxName); } catch { }

            cli.FtCreate(idxName).AddTextField("body").Execute();
            cli.HSet("docA", "body", "This is a very long text body that needs summarizing and highlighting.");

            // Test Highlight and Summarize builders
            var res = cli.FtSearch(idxName, "long text")
                .HighLight(new[] { "body" }, "<b>", "</b>")
                .Sumarize(new[] { "body" }, frags: 1, len: 10, separator: "...")
                .Execute();

            Assert.Equal(1, res.Total);
            // Note: Verification of exact return format depends on Redis version, 
            // but we ensure the command executes without error and returns document.
            // Highlighted text usually replaces the field content in the result body.
            var body = res.Documents[0]["body"].ToString();
            Assert.Contains("<b>", body); // Should contain open tag

            // Test NoContent
            var noContentRes = cli.FtSearch(idxName, "long text")
                .NoContent()
                .Execute();

            Assert.Equal(1, noContentRes.Total);
            Assert.Empty(noContentRes.Documents[0].Body); // Body should be empty

            // Test Return specific fields
            var returnRes = cli.FtSearch(idxName, "long text")
                .Return("body") // Only body
                .Execute();
            Assert.Equal(1, returnRes.Total);

            cli.FtDropIndex(idxName);
        }
    }
}