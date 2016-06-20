package com.java.main.ui;

import com.java.main.action.MainAction;
import com.java.main.context.GetJavaSparkContext;

/**
 * 
 * @author kbaghel 
 * Description -   This class runs wait bar, at the same time in a separate thread
 *         calls data quality check method
 */
public class BackgroundOperationWait {
	public void runWaitBar(final ConnectionDetailsBean ConnDtlsBean,
			final ConfigurationDetailsBean configDtlsBean) {
		final Loading_Test obj1 = new Loading_Test("Quality checking");

		final Thread doQualityCheck = new Thread(new Runnable() {
			public void run() {
				
				
				while (GetJavaSparkContext.getSc() == null){
				}
				try {
					configDtlsBean.setConnectionDetailsBean(ConnDtlsBean);
					MainAction action = new MainAction();
					action.getComparisonResults(configDtlsBean);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				System.out.println("Data quality check job completed!");
				obj1.stopProgressBas();

			}
		});

		doQualityCheck.start(); // Starting Thread
	}
}
