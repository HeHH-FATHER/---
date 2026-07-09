package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.generator.CharacterBuildGenerator;
import org.example.model.CharacterBuildRecord;
import org.example.model.CharacterBuildStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Character Build Generator.
 */
public class AppTest {

    private final ObjectMapper mapper = new ObjectMapper();

    // ── test fixture ─────────────────────────────────────────────────────
    /** Build a small deterministic stats list for unit tests. */
    private List<CharacterBuildStats> makeSimpleStats() {
        // Role A
        CharacterBuildStats roleA = new CharacterBuildStats();
        roleA.setRole("测试角色A");
        roleA.setEname("TestA");
        roleA.setStar(5);
        roleA.setPlayer_count(100);
        roleA.setAvg_level(90);
        roleA.setAvg_constellation(0.5);
        roleA.setTalent_na(3.0);
        roleA.setTalent_skill(9.0);
        roleA.setTalent_burst(9.0);

        Map<String, Double> constDistA = new LinkedHashMap<>();
        constDistA.put("c0", 70.0); constDistA.put("c1", 10.0); constDistA.put("c2", 10.0);
        constDistA.put("c3", 5.0);  constDistA.put("c4", 2.0);  constDistA.put("c5", 1.0);
        constDistA.put("c6", 2.0);
        roleA.setConstellation_dist(constDistA);

        roleA.setAvg_damage(50000);
        roleA.setDamage_type("Q爆发");

        roleA.setWeapons(Arrays.asList(
                new org.example.model.WeaponStat("专武A", 60.0),
                new org.example.model.WeaponStat("通用武1", 25.0),
                new org.example.model.WeaponStat("通用武2", 15.0)
        ));

        roleA.setArtifact_sets(Arrays.asList(
                new org.example.model.ArtifactSetStat("圣遗物A4", 80.0),
                new org.example.model.ArtifactSetStat("圣遗物B4", 15.0),
                new org.example.model.ArtifactSetStat("圣遗物C2", 5.0)
        ));

        // Role B
        CharacterBuildStats roleB = new CharacterBuildStats();
        roleB.setRole("测试角色B");
        roleB.setEname("TestB");
        roleB.setStar(5);
        roleB.setPlayer_count(50);
        roleB.setAvg_level(80);
        roleB.setAvg_constellation(1.0);
        roleB.setTalent_na(6.0);
        roleB.setTalent_skill(8.0);
        roleB.setTalent_burst(10.0);

        Map<String, Double> constDistB = new LinkedHashMap<>();
        constDistB.put("c0", 50.0); constDistB.put("c1", 20.0); constDistB.put("c2", 15.0);
        constDistB.put("c3", 5.0);  constDistB.put("c4", 4.0);  constDistB.put("c5", 3.0);
        constDistB.put("c6", 3.0);
        roleB.setConstellation_dist(constDistB);

        roleB.setAvg_damage(30000);
        roleB.setDamage_type("E总伤害");

        roleB.setWeapons(Arrays.asList(
                new org.example.model.WeaponStat("专武B", 55.0),
                new org.example.model.WeaponStat("通用武3", 30.0),
                new org.example.model.WeaponStat("通用武4", 15.0)
        ));

        roleB.setArtifact_sets(Arrays.asList(
                new org.example.model.ArtifactSetStat("圣遗物D4", 70.0),
                new org.example.model.ArtifactSetStat("圣遗物E4", 30.0)
        ));

        return Arrays.asList(roleA, roleB);
    }

    // ── unit tests (synthetic data) ───────────────────────────────────────

    @Test
    @DisplayName("Record count matches sum of player_count")
    void testExactBuildCount() {
        List<CharacterBuildStats> stats = makeSimpleStats();
        CharacterBuildGenerator gen = new CharacterBuildGenerator(42);
        List<CharacterBuildRecord> records = gen.generate(stats);

        int expected = 100 + 50; // = 150
        assertEquals(expected, records.size());
    }

    @Test
    @DisplayName("Per-character record counts match player_count")
    void testPerCharacterCounts() {
        List<CharacterBuildStats> stats = makeSimpleStats();
        CharacterBuildGenerator gen = new CharacterBuildGenerator(42);
        List<CharacterBuildRecord> records = gen.generate(stats);

        Map<String, Long> actual = new LinkedHashMap<>();
        for (CharacterBuildRecord r : records) {
            actual.merge(r.getRole(), 1L, Long::sum);
        }

        assertEquals(100L, (long) actual.get("测试角色A"));
        assertEquals(50L, (long) actual.get("测试角色B"));
    }

    @Test
    @DisplayName("Levels are within [1, 90]")
    void testLevelConstraints() {
        List<CharacterBuildStats> stats = makeSimpleStats();
        CharacterBuildGenerator gen = new CharacterBuildGenerator(42);
        List<CharacterBuildRecord> records = gen.generate(stats);

        for (CharacterBuildRecord r : records) {
            assertTrue(r.getLevel() >= 1 && r.getLevel() <= 90,
                    () -> "Level " + r.getLevel() + " out of [1, 90] for " + r.getUid());
        }
    }

    @Test
    @DisplayName("Talent levels are within [1, 13]")
    void testTalentConstraints() {
        List<CharacterBuildStats> stats = makeSimpleStats();
        CharacterBuildGenerator gen = new CharacterBuildGenerator(42);
        List<CharacterBuildRecord> records = gen.generate(stats);

        for (CharacterBuildRecord r : records) {
            assertTrue(r.getTalent_na() >= 1 && r.getTalent_na() <= 13,
                    () -> "NA talent " + r.getTalent_na() + " out of [1,13]");
            assertTrue(r.getTalent_skill() >= 1 && r.getTalent_skill() <= 13,
                    () -> "Skill talent " + r.getTalent_skill() + " out of [1,13]");
            assertTrue(r.getTalent_burst() >= 1 && r.getTalent_burst() <= 13,
                    () -> "Burst talent " + r.getTalent_burst() + " out of [1,13]");
        }
    }

    @Test
    @DisplayName("Weapon refinement is within [1, 5]")
    void testWeaponRefinementRange() {
        List<CharacterBuildStats> stats = makeSimpleStats();
        CharacterBuildGenerator gen = new CharacterBuildGenerator(42);
        List<CharacterBuildRecord> records = gen.generate(stats);

        for (CharacterBuildRecord r : records) {
            assertNotNull(r.getWeapon());
            assertNotNull(r.getWeapon().getName());
            assertTrue(r.getWeapon().getRefinement() >= 1 && r.getWeapon().getRefinement() <= 5,
                    () -> "Refinement " + r.getWeapon().getRefinement() + " out of [1,5]");
        }
    }

    @Test
    @DisplayName("UIDs are 9-digit strings starting with 180")
    void testUidFormat() {
        List<CharacterBuildStats> stats = makeSimpleStats();
        CharacterBuildGenerator gen = new CharacterBuildGenerator(42);
        List<CharacterBuildRecord> records = gen.generate(stats);

        for (CharacterBuildRecord r : records) {
            assertNotNull(r.getUid());
            assertEquals(9, r.getUid().length(),
                    () -> "UID " + r.getUid() + " should be 9 chars");
            assertTrue(r.getUid().matches("\\d{9}"),
                    () -> "UID " + r.getUid() + " should be all digits");
        }
    }

    @Test
    @DisplayName("Constellation is within [0, 6]")
    void testConstellationRange() {
        List<CharacterBuildStats> stats = makeSimpleStats();
        CharacterBuildGenerator gen = new CharacterBuildGenerator(42);
        List<CharacterBuildRecord> records = gen.generate(stats);

        for (CharacterBuildRecord r : records) {
            assertTrue(r.getConstellation() >= 0 && r.getConstellation() <= 6,
                    () -> "Constellation " + r.getConstellation() + " out of [0,6]");
        }
    }

    @Test
    @DisplayName("Reproducible output with same seed")
    void testReproducibility() {
        List<CharacterBuildStats> stats = makeSimpleStats();

        CharacterBuildGenerator gen1 = new CharacterBuildGenerator(12345);
        List<CharacterBuildRecord> run1 = gen1.generate(stats);

        CharacterBuildGenerator gen2 = new CharacterBuildGenerator(12345);
        List<CharacterBuildRecord> run2 = gen2.generate(stats);

        assertEquals(run1.size(), run2.size());
        for (int i = 0; i < run1.size(); i++) {
            assertEquals(run1.get(i).getUid(), run2.get(i).getUid());
            assertEquals(run1.get(i).getRole(), run2.get(i).getRole());
            assertEquals(run1.get(i).getConstellation(), run2.get(i).getConstellation());
            assertEquals(run1.get(i).getWeapon().getName(), run2.get(i).getWeapon().getName());
            assertEquals(run1.get(i).getWeapon().getRefinement(),
                         run2.get(i).getWeapon().getRefinement());
            assertEquals(run1.get(i).getArtifact_set().getName(),
                         run2.get(i).getArtifact_set().getName());
        }
    }

    // ── integration test (real data) ──────────────────────────────────────

    @Test
    @DisplayName("Verify with real data — constraints hold for scaled generation")
    void testWithRealData() throws Exception {
        CharacterBuildStats[] statsArray = mapper.readValue(
                getClass().getResourceAsStream("角色练度统计.json"),
                CharacterBuildStats[].class);
        List<CharacterBuildStats> statsList = Arrays.asList(statsArray);

        CharacterBuildGenerator gen = new CharacterBuildGenerator(42);
        // Use scaled generation for performance — real data has millions of players
        List<CharacterBuildRecord> records = gen.generate(statsList, 5000);

        // Verify all records have valid fields
        for (CharacterBuildRecord r : records) {
            assertNotNull(r.getRole());
            assertNotNull(r.getUid());
            assertEquals(9, r.getUid().length());
            assertTrue(r.getLevel() >= 1 && r.getLevel() <= 90);
            assertTrue(r.getConstellation() >= 0 && r.getConstellation() <= 6);
            assertTrue(r.getTalent_na() >= 1 && r.getTalent_na() <= 13);
            assertTrue(r.getTalent_skill() >= 1 && r.getTalent_skill() <= 13);
            assertTrue(r.getTalent_burst() >= 1 && r.getTalent_burst() <= 13);
            assertNotNull(r.getWeapon());
            assertNotNull(r.getWeapon().getName());
            assertTrue(r.getWeapon().getRefinement() >= 1 && r.getWeapon().getRefinement() <= 5);
            assertNotNull(r.getArtifact_set());
            assertNotNull(r.getArtifact_set().getName());
        }

        // Build lookup tables: character → set of valid weapons/artifacts
        // (based on the top-N lists in the input statistics).
        Map<String, Set<String>> validWeapons = new LinkedHashMap<>();
        Map<String, Set<String>> validArtifacts = new LinkedHashMap<>();
        for (CharacterBuildStats s : statsList) {
            Set<String> wset = new LinkedHashSet<>();
            for (org.example.model.WeaponStat w : s.getWeapons()) {
                wset.add(w.getName());
            }
            validWeapons.put(s.getRole(), wset);

            Set<String> aset = new LinkedHashSet<>();
            for (org.example.model.ArtifactSetStat a : s.getArtifact_sets()) {
                aset.add(a.getName());
            }
            validArtifacts.put(s.getRole(), aset);
        }

        for (CharacterBuildRecord r : records) {
            assertTrue(validWeapons.get(r.getRole()).contains(r.getWeapon().getName()),
                    () -> r.getRole() + " has invalid weapon: " + r.getWeapon().getName());
            assertTrue(validArtifacts.get(r.getRole()).contains(r.getArtifact_set().getName()),
                    () -> r.getRole() + " has invalid artifact: " + r.getArtifact_set().getName());
        }

        System.out.println("\nFull real data test:");
        System.out.println(CharacterBuildGenerator.summarize(records));
    }
}
