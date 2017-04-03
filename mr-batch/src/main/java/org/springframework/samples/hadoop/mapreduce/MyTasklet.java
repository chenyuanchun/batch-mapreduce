package org.springframework.samples.hadoop.mapreduce;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class MyTasklet implements Tasklet {
	private final HashtagCount mrJob;
	public MyTasklet(HashtagCount mr) {
		mrJob = mr;
	}

	@Override
	public RepeatStatus execute(StepContribution arg0, ChunkContext arg1) throws Exception {
		mrJob.run(null);
		return RepeatStatus.FINISHED;
	}

}
