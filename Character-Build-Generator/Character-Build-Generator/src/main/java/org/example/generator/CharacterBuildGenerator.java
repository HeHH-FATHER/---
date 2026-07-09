package org.example.generator;

import org.example.model.*;

import java.util.*;

/**
 * Generates realistic per-player character build records from aggregate statistics.
 *
 * <p>Key behavioral rules:
 * <ul>
 *   <li>Constellation is assigned per-player via weighted random from {@code constellation_dist}</li>
 *   <li>Weapon is chosen per-player via weighted random from {@code weapons[].rate}</li>
 *   <li>Artifact set is chosen per-player via weighted random from {@code artifact_sets[].rate}</li>
 *   <li>Weapon refinement: R1(80%), R2(8%), R3(5%), R4(3%), R5(4%)</li>
 *   <li>Level varies ±2 around {@code avg_level}, clamped to [1, 90]</li>
 *   <li>Talent levels vary ±2 around their averages, clamped to [1, 10+constellation bonus]</li>
 *   <li>Damage varies ±15% around {@code avg_damage}</li>
 * </ul>
 */
public class CharacterBuildGenerator {

    private final Random rng;

    /**
     * Weapon refinement level distribution.
     * R1 (1 ref) ~80%, R2 ~8%, R3 ~5%, R4 ~3%, R5 (max) ~4%.
     * Most players stop at R1; only whales chase R5.
     */
    private static final int[] REFINE_LEVELS = {1, 2, 3, 4, 5};
    private static final double[] REFINE_WEIGHTS = {0.80, 0.08, 0.05, 0.03, 0.04};

    /** Constellation levels 0–6 and their JSON keys in the input distribution map. */
    private static final int[] CONSTELLATION_VALUES = {0, 1, 2, 3, 4, 5, 6};
    private static final String[] CONSTELLATION_KEYS = {"c0", "c1", "c2", "c3", "c4", "c5", "c6"};

    public CharacterBuildGenerator() { this.rng = new Random(); }
    public CharacterBuildGenerator(long seed) { this.rng = new Random(seed); }

    // ── main entry ────────────────────────────────────────────────────────

    /**
     * Generate build records for all characters (full scale, no limit).
     */
    public List<CharacterBuildRecord> generate(List<CharacterBuildStats> statsList) {
        return generate(statsList, 0);
    }

    /**
     * Generate build records for all characters.
     * @param statsList  per-character aggregate statistics
     * @param maxRecords if > 0, scale down proportionally to approximately this many records
     */
    public List<CharacterBuildRecord> generate(List<CharacterBuildStats> statsList, int maxRecords) {
        List<CharacterBuildRecord> allRecords = new ArrayList<>();

        if (maxRecords > 0) {
            // ── proportional allocation via largest-remainder method ──────────
            // Distribute exactly maxRecords slots across characters weighted by
            // player_count. Characters that round to 0 under pure proportional
            // scaling still get slots if their fractional remainder is large enough.
            int total = 0;
            for (CharacterBuildStats s : statsList) {
                total += s.getPlayer_count();
            }

            // Pass 1: floor allocation
            int[] quotas = new int[statsList.size()];
            double[] remainders = new double[statsList.size()];
            int allocated = 0;

            for (int i = 0; i < statsList.size(); i++) {
                double share = (double) statsList.get(i).getPlayer_count() * maxRecords / total;
                quotas[i] = (int) share;
                remainders[i] = share - quotas[i];
                allocated += quotas[i];
            }

            // Pass 2: distribute remaining slots by largest fractional remainder
            int remaining = maxRecords - allocated;
            // Sort indices by descending remainder (stable for reproducibility)
            Integer[] indices = new Integer[statsList.size()];
            for (int i = 0; i < indices.length; i++) indices[i] = i;
            Arrays.sort(indices, (a, b) -> Double.compare(remainders[b], remainders[a]));

            for (int j = 0; j < remaining && j < indices.length; j++) {
                quotas[indices[j]]++;
            }

            // Generate records using the allocated quotas
            for (int i = 0; i < statsList.size(); i++) {
                if (quotas[i] > 0) {
                    allRecords.addAll(generateForCharacter(statsList.get(i), quotas[i]));
                }
            }
        } else {
            // Full scale: generate every character's full player_count
            for (CharacterBuildStats stats : statsList) {
                allRecords.addAll(generateForCharacter(stats, stats.getPlayer_count()));
            }
        }

        // Shuffle to interleave 4★/5★ characters naturally
        Collections.shuffle(allRecords, rng);
        return allRecords;
    }

    // ── per-character generation ──────────────────────────────────────────

    /**
     * Generate exactly {@code count} build records for one character.
     *
     * <p>Pipeline per player: constellation → weapon → artifact → level
     * → talent levels → damage. Each step uses weighted random selection
     * based on the aggregate distributions in the input statistics.
     */
    private List<CharacterBuildRecord> generateForCharacter(CharacterBuildStats stats, int count) {
        List<CharacterBuildRecord> records = new ArrayList<>();
        String role = stats.getRole();
        String ename = stats.getEname();
        int star = stats.getStar();
        String damageType = stats.getDamage_type();

        // Convert lists to arrays once for faster indexed access in the loop
        WeaponStat[] weapons = stats.getWeapons() != null
                ? stats.getWeapons().toArray(new WeaponStat[0]) : new WeaponStat[0];
        ArtifactSetStat[] artifacts = stats.getArtifact_sets() != null
                ? stats.getArtifact_sets().toArray(new ArtifactSetStat[0]) : new ArtifactSetStat[0];

        Map<String, Double> constDist = stats.getConstellation_dist();

        for (int i = 0; i < count; i++) {
            String uid = newUid();

            // 1. Constellation (0–6): weighted by constellation_dist percentages
            int constellation = pickConstellation(constDist);

            // 2. Weapon: weighted by usage rate, refinement by R1/R5 distribution
            String weaponName = pickWeapon(weapons);
            int refinement = pickWeighted(REFINE_LEVELS, REFINE_WEIGHTS);
            WeaponChoice weapon = new WeaponChoice(weaponName, refinement);

            // 3. Artifact set: weighted by usage rate
            String artifactName = pickArtifact(artifacts);
            ArtifactSetChoice artifactSet = new ArtifactSetChoice(artifactName);

            // 4. Character level: vary ±2 around the average, clamp to [1, 90]
            int level = varyInt(stats.getAvg_level(), 2, 1, 90);

            // 5. Talent levels — base cap 10.
            //    Constellation bonus: C3 → +3 to Skill, C5 → +3 to Burst.
            int naCap = 10;  // Normal Attack not boosted by constellations
            int skillCap = (constellation >= 3) ? 13 : 10;
            int burstCap = (constellation >= 5) ? 13 : 10;

            int talentNa = varyInt(stats.getTalent_na(), 2, 1, naCap);
            int talentSkill = varyInt(stats.getTalent_skill(), 2, 1, skillCap);
            int talentBurst = varyInt(stats.getTalent_burst(), 2, 1, burstCap);

            // 6. Average damage: vary ±15% around the reported average
            double damageFactor = 0.85 + rng.nextDouble() * 0.30; // [0.85, 1.15]
            int damage = (int) Math.round(stats.getAvg_damage() * damageFactor);

            records.add(new CharacterBuildRecord(
                    role, ename, star, uid,
                    level, constellation,
                    talentNa, talentSkill, talentBurst,
                    damage, damageType,
                    weapon, artifactSet));
        }

        return records;
    }

    // ── pickers ───────────────────────────────────────────────────────────

    /** Pick a constellation level (0–6) based on percentage distribution. */
    private int pickConstellation(Map<String, Double> constDist) {
        if (constDist == null || constDist.isEmpty()) return 0;

        double[] weights = new double[CONSTELLATION_KEYS.length];
        for (int i = 0; i < CONSTELLATION_KEYS.length; i++) {
            Double pct = constDist.get(CONSTELLATION_KEYS[i]);
            weights[i] = (pct != null) ? pct : 0.0;
        }
        // Normalize in case percentages don't sum to 100
        double sum = 0;
        for (double w : weights) sum += w;
        if (sum <= 0) return 0;
        for (int i = 0; i < weights.length; i++) weights[i] /= sum;

        return pickWeighted(CONSTELLATION_VALUES, weights);
    }

    /** Pick a weapon by weighted rate. */
    private String pickWeapon(WeaponStat[] weapons) {
        if (weapons.length == 0) return "无武器";

        double[] weights = new double[weapons.length];
        for (int i = 0; i < weapons.length; i++) weights[i] = weapons[i].getRate();
        int idx = pickWeightedIndex(weights);
        return weapons[idx].getName();
    }

    /** Pick an artifact set by weighted rate. */
    private String pickArtifact(ArtifactSetStat[] artifacts) {
        if (artifacts.length == 0) return "无圣遗物";

        double[] weights = new double[artifacts.length];
        for (int i = 0; i < artifacts.length; i++) weights[i] = artifacts[i].getRate();
        int idx = pickWeightedIndex(weights);
        return artifacts[idx].getName();
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private long uidCounter = 0;

    /** Generate a new unique UID string (9 digits, starts with 180). */
    private String newUid() {
        return String.valueOf(180000000L + (uidCounter++));
    }

    /**
     * Vary an integer value randomly within ±delta, clamped to [min, max].
     * The input value can be a double (average); we round after varying.
     */
    private int varyInt(double avg, int delta, int min, int max) {
        int base = (int) Math.round(avg);
        int low = Math.max(min, base - delta);
        int high = Math.min(max, base + delta);
        if (low >= high) return Math.max(min, Math.min(max, base));
        return low + rng.nextInt(high - low + 1);
    }

    /** Pick a random value from options using the given weight distribution. */
    private int pickWeighted(int[] options, double[] weights) {
        double r = rng.nextDouble();
        double cum = 0;
        for (int i = 0; i < options.length; i++) {
            cum += weights[i];
            if (r <= cum) return options[i];
        }
        return options[options.length - 1];
    }

    /**
     * Pick a random index from a weight array (weights do not need to sum to 1).
     * Unlike {@link #pickWeighted}, this works with raw percentage values directly.
     */
    private int pickWeightedIndex(double[] weights) {
        double sum = 0;
        for (double w : weights) sum += w;
        if (sum <= 0) return 0;

        double r = rng.nextDouble() * sum;
        double cum = 0;
        for (int i = 0; i < weights.length; i++) {
            cum += weights[i];
            if (r <= cum) return i;
        }
        return weights.length - 1;
    }

    // ── verification ──────────────────────────────────────────────────────

    /**
     * Verify generated records against input statistics.
     * @return map of discrepancies (empty = perfect fit)
     */
    public static Map<String, Integer> verify(List<CharacterBuildRecord> records,
                                               List<CharacterBuildStats> statsList) {
        Map<String, Integer> discrepancies = new LinkedHashMap<>();

        // Count actual records per character
        Map<String, Integer> actualCounts = new LinkedHashMap<>();
        for (CharacterBuildRecord r : records) {
            actualCounts.merge(r.getRole(), 1, Integer::sum);
        }

        for (CharacterBuildStats s : statsList) {
            int expected = s.getPlayer_count();
            int actual = actualCounts.getOrDefault(s.getRole(), 0);
            if (expected != actual) {
                discrepancies.put(s.getRole(), actual - expected);
            }
        }

        return discrepancies;
    }

    /** Print summary statistics. */
    public static String summarize(List<CharacterBuildRecord> records) {
        int total = records.size();
        long uniqueUids = records.stream().map(CharacterBuildRecord::getUid).distinct().count();

        // Per-character breakdown
        Map<String, Long> perChar = new LinkedHashMap<>();
        for (CharacterBuildRecord r : records) {
            perChar.merge(r.getRole(), 1L, Long::sum);
        }

        // Constellation stats
        LongSummaryStatistics constStats = records.stream()
                .mapToLong(CharacterBuildRecord::getConstellation).summaryStatistics();

        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "Total records: %d\nUnique players: %d\nConstellation: min=%d, max=%d, avg=%.2f",
                total, uniqueUids, constStats.getMin(), constStats.getMax(), constStats.getAverage()));

        sb.append("\nCharacters:");
        for (Map.Entry<String, Long> e : perChar.entrySet()) {
            sb.append(String.format("\n  %s: %d", e.getKey(), e.getValue()));
        }

        return sb.toString();
    }
}
