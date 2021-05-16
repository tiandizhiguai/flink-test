package com.test.union;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 统计单个短视频粒度的「点赞，播放，评论，分享，举报」五类实时指标，并且汇总成 photo_id，
 * 1分钟时间粒度的实时视频消费宽表（即宽表字段至少为：「photo_id + play_cnt + like_cnt
 *  + comment_cnt + share_cnt + negative_cnt + minute_timestamp」）产出至实时大屏。
 */
public class PhotoTask {

	public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Tuple2<Long, String> -> photo_id + "PLAY"标签
        DataStream<Tuple2<Long, String>> play = env.fromElements(Tuple2.of(1L, "play"));
        // Tuple2<Long, String> -> photo_id + "LIKE"标签
        DataStream<Tuple2<Long, String>> like = env.fromElements(Tuple2.of(1L, "like"));
        // Tuple2<Long, String> -> photo_id + "COMMENT"标签
        DataStream<Tuple2<Long, String>> comment = env.fromElements(Tuple2.of(1L, "comment"));
        // Tuple2<Long, String> -> photo_id + "SHARE"标签
        DataStream<Tuple2<Long, String>> share = env.fromElements(Tuple2.of(1L, "share"));
        // Tuple2<Long, String> -> photo_id + "NEGATIVE"标签
        DataStream<Tuple2<Long, String>> negative = env.fromElements(Tuple2.of(1L, "negative"));

        // Tuple5<Long, Long, Long, Long> -> photo_id + play_cnt + like_cnt + comment_cnt + window_start_timestamp
        DataStream<Tuple3<Long, Long, Long>> playAndLikeCnt = play
            .union(like)
            .union(comment)
            .union(share)
            .union(negative)
            .keyBy(e -> e.f0)
            .timeWindow(Time.seconds(60))
            .process(null);
        playAndLikeCnt.print();

        env.execute();
    }
}
